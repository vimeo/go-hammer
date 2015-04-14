package hammer

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/bmizerany/perks/quantile"
)

type RequestCallback func(*http.Response, Result)

type Request struct {
	HTTPRequest *http.Request
	Name        string
	Callback    RequestCallback
}

type RequestGenerator func(*Hammer, chan<- Request, <-chan int)

type Hammer struct {
	RunFor           float64
	Threads          int
	Backlog          int
	QPS              float64
	ReadBody         bool
	LogErrors        bool
	GenerateFunction RequestGenerator
}

type Result struct {
	Name       string
	Status     int
	Start      time.Time
	GotHeaders time.Time
	GotBody    time.Time
}

func (hammer *Hammer) warn(msg string) {
	log.Println(msg)
}

func (hammer *Hammer) warnf(fmt string, args ...interface{}) {
	log.Printf(fmt, args...)
}

func (hammer *Hammer) sendRequests(requests <-chan Request, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	client := &http.Client{}

	for req := range requests {
		var result Result
		result.Name = req.Name
		result.Start = time.Now()
		res, err := client.Do(req.HTTPRequest)
		result.GotHeaders = time.Now()
		if err != nil {
			result.Status = 499
			result.GotBody = result.GotHeaders
			hammer.warn(err.Error())
		} else {
			result.Status = res.StatusCode
			if result.Status >= 400 {
				if hammer.LogErrors {
					// TODO: refactor this into a method
					logOut, err := ioutil.TempFile(".", "error.log.")
					if err == nil {
						res.Write(logOut)
					} else {
						hammer.warnf("%s writing error log\n", err.Error())
					}
				} else if hammer.ReadBody {
					io.Copy(ioutil.Discard, res.Body)
				}
				result.GotBody = time.Now()
				hammer.warnf("Got status %s for %s\n", res.Status, req.HTTPRequest.URL.String())
			} else if hammer.ReadBody {
				io.Copy(ioutil.Discard, res.Body)
				result.GotBody = time.Now()
			} else {
				res.Body.Close()
			}
		}
		if req.Callback != nil {
			go req.Callback(res, result)
		}
		results <- result
	}
}

type Stats struct {
	Name           string
	Begin          time.Time
	End            time.Time
	Statuses       map[int]int
	HeaderStats    BasicStats
	HeaderQuantile quantile.Stream
	BodyStats      BasicStats
	BodyQuantile   quantile.Stream
}

func newStats(name string, quantiles ...float64) *Stats {
	return &Stats{
		Name:           name,
		Statuses:       make(map[int]int),
		HeaderStats:    BasicStats{},
		HeaderQuantile: *(quantile.NewTargeted(quantiles...)),
		BodyStats:      BasicStats{},
		BodyQuantile:   *(quantile.NewTargeted(quantiles...)),
	}
}

func (hammer *Hammer) ReportPrinter(format string) func(Stats) {
	return func(stats Stats) {
		file, err := os.Create(fmt.Sprintf(format, stats.Name))
		if err != nil {
			hammer.warn(err.Error())
			return
		}
		runTime := stats.End.Sub(stats.Begin).Seconds()
		count := stats.HeaderStats.Count
		fmt.Fprintf(
			file,
			`Hammer REPORT FOR %s:

Run time: %.3f
Total hits: %.0f
Hits/sec: %.3f

Status totals:
`,
			stats.Name,
			runTime,
			count,
			count/runTime,
		)
		statusCodes := []int{}
		for code, _ := range stats.Statuses {
			statusCodes = append(statusCodes, code)
		}
		sort.Ints(statusCodes)
		for _, code := range statusCodes {
			fmt.Fprintf(file, "%d\t%d\t%.3f\n", code, stats.Statuses[code], 100*float64(stats.Statuses[code])/count)
		}
		if count > 0 {
			fmt.Fprintf(
				file,
				"\nFirst byte mean +/- SD: %.2f +/- %.2f ms\n",
				1000*stats.HeaderStats.Mean(),
				1000*stats.HeaderStats.StdDev(),
			)
			fmt.Fprintf(
				file,
				"First byte 5-95 pct: (%.2f, %.2f) ms\n",
				1000*stats.HeaderQuantile.Query(0.05),
				1000*stats.HeaderQuantile.Query(0.95),
			)
			if hammer.ReadBody {
				fmt.Fprintf(
					file,
					"\nFull response mean +/- SD: %.2f +/- %.2f ms\n",
					1000*stats.BodyStats.Mean(),
					1000*stats.BodyStats.StdDev(),
				)
				fmt.Fprintf(
					file,
					"First byte 5-95 pct: (%.2f, %.2f) ms\n",
					1000*stats.BodyQuantile.Query(0.05),
					1000*stats.BodyQuantile.Query(0.95),
				)
			}
		}
		file.Close()
	}
}

func (hammer *Hammer) StatsPrinter(filename string) func(Stats) {
	return func(stats Stats) {
		statsFile, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			hammer.warn(err.Error())
			return
		}
		runTime := stats.End.Sub(stats.Begin).Seconds()
		count := stats.HeaderStats.Count
		fmt.Fprintf(
			statsFile,
			"%s\t%d\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f",
			stats.Name,
			hammer.Threads,
			hammer.QPS,
			runTime,
			count,
			count/runTime,
			1000*stats.HeaderStats.Mean(),
			1000*stats.HeaderStats.StdDev(),
			1000*stats.HeaderQuantile.Query(0.05),
			1000*stats.HeaderQuantile.Query(0.95),
		)
		if hammer.ReadBody {
			fmt.Fprintf(
				statsFile,
				"%f\t%f\t%f\t%f\n",
				1000*stats.BodyStats.Mean(),
				1000*stats.BodyStats.StdDev(),
				1000*stats.BodyQuantile.Query(0.05),
				1000*stats.BodyQuantile.Query(0.95),
			)
		} else {
			fmt.Fprintf(statsFile, "\n")
		}
		statsFile.Close()
	}
}

func (hammer *Hammer) collectResults(results <-chan Result, statschan chan<- Stats, wg *sync.WaitGroup) {
	defer wg.Done()

	statsMap := map[string]*Stats{}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	defer func() {
		for _, stats := range statsMap {
			statschan <- *stats
		}
		close(statschan)
	}()

	for {
		select {
		case res, ok := <-results:
			if !ok {
				return
			}

			stats, statsExisted := statsMap[res.Name]
			if !statsExisted {
				stats = newStats(res.Name, 0.05, 0.95)
				statsMap[res.Name] = stats
			}

			stats.Statuses[res.Status]++

			start := res.Start
			end := res.GotHeaders
			dur := end.Sub(start).Seconds()
			stats.HeaderStats.Add(dur)
			stats.HeaderQuantile.Insert(dur)
			if hammer.ReadBody {
				end = res.GotBody
				dur := end.Sub(start).Seconds()
				stats.BodyStats.Add(dur)
				stats.BodyQuantile.Insert(dur)
			}
			if !statsExisted {
				stats.Begin = start
				stats.End = end
			} else {
				if start.Before(stats.Begin) {
					stats.Begin = start
				}
				if start.After(stats.End) {
					stats.End = start
				}
			}
		case <-ticker.C:
			for _, stats := range statsMap {
				statschan <- *stats
			}
		}
	}
}

func RandomURLGenerator(name string, URLs []string, Headers map[string][]string) RequestGenerator {
	readiedRequests := make([]Request, len(URLs))
	for i, url := range URLs {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			panic(err)
		}
		req.Header = Headers
		readiedRequests[i] = Request{
			HTTPRequest: req,
			Name:        name,
		}
	}
	num := len(readiedRequests)

	return func(hammer *Hammer, requests chan<- Request, exit <-chan int) {
		defer func() { close(requests) }()

		ticker := time.NewTicker(time.Duration(float64(time.Second) / hammer.QPS))
		defer ticker.Stop()

		for {
			select {
			case <-exit:
				return
			case <-ticker.C:
				var idx int
				if num == 1 {
					idx = 0
				} else {
					idx = rand.Intn(len(readiedRequests))
				}
				requests <- readiedRequests[idx]
			}
		}
	}
}

func (hammer *Hammer) Run(statschan chan<- Stats) {
	exit := make(chan int)
	var requestWorkers, finishedResults sync.WaitGroup

	requests := make(chan Request, hammer.Backlog)
	results := make(chan Result, hammer.Threads*2)

	for i := 0; i < hammer.Threads; i++ {
		requestWorkers.Add(1)
		go hammer.sendRequests(requests, results, &requestWorkers)
	}
	finishedResults.Add(1)
	go hammer.collectResults(results, statschan, &finishedResults)
	go hammer.GenerateFunction(hammer, requests, exit)
	go func() {
		requestWorkers.Wait()
		close(results)
	}()

	// Give it time to run...
	time.Sleep(time.Duration(hammer.RunFor * float64(time.Second)))
	// And then signal GenerateRequests to stop.
	close(exit)
	finishedResults.Wait()
}
