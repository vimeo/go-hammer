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

type Request *http.Request

type RequestGenerator func(*Hammer, chan<- Request, <-chan int)

type Hammer struct {
	Name             string
	RunFor           float64
	Threads          int
	Backlog          int
	QPS              float64
	ReadBody         bool
	LogErrors        bool
	GenerateFunction RequestGenerator
}

type result struct {
	Status     int
	Start      time.Time
	GotHeaders time.Time
	GotBody    time.Time
}

func (hammer *Hammer) warn(msg string) {
	log.Println(msg)
}

func (hammer *Hammer) warnf(fmt string, args ...interface{}) {
	log.Printf(fmt, args)
}

func (hammer *Hammer) sendRequests(requests <-chan Request, results chan<- result, wg *sync.WaitGroup) {
	defer wg.Done()

	client := &http.Client{}

	for req := range requests {
		var result result
		result.Start = time.Now()
		res, err := client.Do(req)
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
				}
				hammer.warnf("Got status %s for %s\n", res.Status, req.URL.String())
			} else if hammer.ReadBody {
				io.Copy(ioutil.Discard, res.Body)
				result.GotBody = time.Now()
			} else {
				res.Body.Close()
			}
		}
		results <- result
	}
}

type Stats struct {
	begin          time.Time
	end            time.Time
	statuses       map[int]int
	headerStats    BasicStats
	headerQuantile quantile.Stream
	bodyStats      BasicStats
	bodyQuantile   quantile.Stream
}

func newStats(name string, quantiles ...float64) Stats {
	return Stats{
		statuses:       make(map[int]int),
		headerStats:    BasicStats{},
		headerQuantile: *(quantile.NewTargeted(quantiles...)),
		bodyStats:      BasicStats{},
		bodyQuantile:   *(quantile.NewTargeted(quantiles...)),
	}
}

func (hammer *Hammer) ReportPrinter(format string) func(Stats) {
	return func(stats Stats) {
		file, err := os.Create(fmt.Sprintf(format, hammer.Name))
		if err != nil {
			hammer.warn(err.Error())
			return
		}
		runTime := stats.end.Sub(stats.begin).Seconds()
		count := stats.headerStats.Count
		fmt.Fprintf(
			file,
			`Hammer REPORT FOR %s:

Run time: %.3f
Total hits: %.0f
Hits/sec: %.3f

Status totals:
`,
			hammer.Name,
			runTime,
			count,
			count/runTime,
		)
		statusCodes := []int{}
		for code, _ := range stats.statuses {
			statusCodes = append(statusCodes, code)
		}
		sort.Ints(statusCodes)
		for _, code := range statusCodes {
			fmt.Fprintf(file, "%d\t%d\n", code, stats.statuses[code])
		}
		if count > 0 {
			fmt.Fprintf(
				file,
				"\nFirst byte mean +/- SD: %.2f +/- %.2f ms\n",
				1000*stats.headerStats.Mean(),
				1000*stats.headerStats.StdDev(),
			)
			fmt.Fprintf(
				file,
				"First byte 5-95 pct: (%.2f, %.2f) ms\n",
				1000*stats.headerQuantile.Query(0.05),
				1000*stats.headerQuantile.Query(0.95),
			)
			if hammer.ReadBody {
				fmt.Fprintf(
					file,
					"\nFull response mean +/- SD: %.2f +/- %.2f ms\n",
					1000*stats.bodyStats.Mean(),
					1000*stats.bodyStats.StdDev(),
				)
				fmt.Fprintf(
					file,
					"First byte 5-95 pct: (%.2f, %.2f) ms\n",
					1000*stats.bodyQuantile.Query(0.05),
					1000*stats.bodyQuantile.Query(0.95),
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
		runTime := stats.end.Sub(stats.begin).Seconds()
		count := stats.headerStats.Count
		fmt.Fprintf(
			statsFile,
			"%s\t%d\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f",
			hammer.Name,
			hammer.Threads,
			hammer.QPS,
			runTime,
			count,
			count/runTime,
			1000*stats.headerStats.Mean(),
			1000*stats.headerStats.StdDev(),
			1000*stats.headerQuantile.Query(0.05),
			1000*stats.headerQuantile.Query(0.95),
		)
		if hammer.ReadBody {
			fmt.Fprintf(
				statsFile,
				"%f\t%f\t%f\t%f\n",
				1000*stats.bodyStats.Mean(),
				1000*stats.bodyStats.StdDev(),
				1000*stats.bodyQuantile.Query(0.05),
				1000*stats.bodyQuantile.Query(0.95),
			)
		} else {
			fmt.Fprintf(statsFile, "\n")
		}
		statsFile.Close()
	}
}

func (hammer *Hammer) collectResults(results <-chan result, statschan chan<- Stats, wg *sync.WaitGroup) {
	defer wg.Done()

	var timeinit bool
	stats := newStats(hammer.Name, 0.05, 0.95)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	defer func() {
		statschan <- stats
		close(statschan)
	}()

	for {
		select {
		case res, ok := <-results:
			if !ok {
				return
			}
			stats.statuses[res.Status]++

			start := res.Start
			end := res.GotHeaders
			dur := end.Sub(start).Seconds()
			stats.headerStats.Add(dur)
			stats.headerQuantile.Insert(dur)
			if hammer.ReadBody {
				end = res.GotBody
				dur := end.Sub(start).Seconds()
				stats.bodyStats.Add(dur)
				stats.bodyQuantile.Insert(dur)
			}
			if !timeinit {
				stats.begin = start
				stats.end = end
				timeinit = true
			} else {
				if start.Before(stats.begin) {
					stats.begin = start
				}
				if start.After(stats.end) {
					stats.end = start
				}
			}
		case <-ticker.C:
			statschan <- stats
		}
	}
}

func RandomURLGenerator(URLs []string, Headers map[string][]string) RequestGenerator {
	readiedRequests := make([]Request, len(URLs))
	for i, url := range URLs {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			panic(err)
		}
		req.Header = Headers
		readiedRequests[i] = req
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
	results := make(chan result, hammer.Threads*2)

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
