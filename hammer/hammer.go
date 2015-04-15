package hammer

import (
	"bytes"
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

	"github.com/arodland/go-descriptivestats/descriptivestats"
	"github.com/bmizerany/perks/quantile"
)

type Hammer struct {
	RunFor           float64
	Threads          int
	Backlog          int
	QPS              float64
	LogErrors        bool
	GenerateFunction RequestGenerator
	exit             chan int
	requests         chan Request
	throttled        chan Request
	results          chan Result
	stats            chan StatsSummary
	requestWorkers   sync.WaitGroup
	finishedResults  sync.WaitGroup
}

func (hammer *Hammer) warn(msg string) {
	log.Println(msg)
}

func (hammer *Hammer) warnf(fmt string, args ...interface{}) {
	log.Printf(fmt, args...)
}

type BodyBehavior int

const (
	IgnoreBody BodyBehavior = iota
	DiscardBody
	CopyBody
)

type Request struct {
	HTTPRequest  *http.Request
	Name         string
	BodyBehavior BodyBehavior
	Callback     RequestCallback
}

type RequestCallback func(Request, *http.Response, Result)

type RequestGenerator func(*Hammer)

type RandomURLGeneratorOptions struct {
	URLs         []string
	Headers      map[string][]string
	Name         string
	BodyBehavior BodyBehavior
	Callback     RequestCallback
}

func RandomURLGenerator(opts RandomURLGeneratorOptions) RequestGenerator {
	readiedRequests := make([]Request, len(opts.URLs))
	for i, url := range opts.URLs {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			panic(err)
		}
		req.Header = opts.Headers
		readiedRequests[i] = Request{
			BodyBehavior: opts.BodyBehavior,
			HTTPRequest:  req,
			Name:         opts.Name,
			Callback:     opts.Callback,
		}
	}
	num := len(readiedRequests)

	return func(hammer *Hammer) {
		defer func() { close(hammer.requests) }()

		for {
			select {
			case <-hammer.exit:
				return
			default:
				var idx int
				if num == 1 {
					idx = 0
				} else {
					idx = rand.Intn(len(readiedRequests))
				}
				hammer.requests <- readiedRequests[idx]
			}
		}
	}
}

func (hammer *Hammer) SendRequest(req Request) {
	hammer.requests <- req
}

func (hammer *Hammer) SendRequestImmediately(req Request) {
	hammer.throttled <- req
}

func (hammer *Hammer) throttle() {
	ticker := time.NewTicker(time.Duration(float64(time.Second) / hammer.QPS))
	defer ticker.Stop()
	defer close(hammer.throttled)

	for {
		select {
		case <-hammer.exit:
			return
		case <-ticker.C:
			req := <-hammer.requests
			hammer.throttled <- req
		}
	}
}

type Result struct {
	Name       string
	Status     int
	Start      time.Time
	GotHeaders time.Time
	GotBody    time.Time
}

func copyResponseBody(res *http.Response) (gotBody time.Time) {
	var buf bytes.Buffer
	io.Copy(&buf, res.Body)
	gotBody = time.Now()
	res.Body = ioutil.NopCloser(bytes.NewReader(buf.Bytes()))
	return
}

func (hammer *Hammer) sendRequests() {
	defer hammer.requestWorkers.Done()

	client := &http.Client{}

	for req := range hammer.throttled {
		var result Result
		result.Name = req.Name
		result.Start = time.Now()
		res, err := client.Do(req.HTTPRequest)
		result.GotHeaders = time.Now()
		if err != nil {
			result.Status = 499
			hammer.warn(err.Error())
		} else {
			result.Status = res.StatusCode
			if result.Status >= 400 {
				if req.BodyBehavior == CopyBody {
					result.GotBody = copyResponseBody(res)
				}
				if hammer.LogErrors {
					// TODO: refactor this into a method
					logOut, err := ioutil.TempFile(".", "error.log.")
					if err == nil {
						res.Write(logOut)
						result.GotBody = time.Now()
					} else {
						hammer.warnf("%s writing error log\n", err.Error())
					}
				} else if req.BodyBehavior == DiscardBody {
					io.Copy(ioutil.Discard, res.Body)
					result.GotBody = time.Now()
				}
				hammer.warnf("Got status %s for %s\n", res.Status, req.HTTPRequest.URL.String())
			} else if req.BodyBehavior == CopyBody {
				result.GotBody = copyResponseBody(res)
			} else if req.BodyBehavior == DiscardBody {
				io.Copy(ioutil.Discard, res.Body)
				result.GotBody = time.Now()
			} else {
				res.Body.Close()
			}
		}
		if req.Callback != nil {
			go req.Callback(req, res, result)
		}
		hammer.results <- result
	}
}

type Stats struct {
	Name           string
	Quantiles      []float64
	Begin          time.Time
	End            time.Time
	Statuses       map[int]int
	HeaderStats    descriptivestats.Stats
	HeaderQuantile quantile.Stream
	BodyStats      descriptivestats.Stats
	BodyQuantile   quantile.Stream
}

type SingleStatSummary struct {
	descriptivestats.Stats
	Quantiles map[float64]float64
}

type StatsSummary struct {
	Name     string
	Begin    time.Time
	End      time.Time
	Statuses map[int]int
	Headers  SingleStatSummary
	Body     SingleStatSummary
}

func newStats(name string, quantiles ...float64) *Stats {
	return &Stats{
		Name:           name,
		Quantiles:      quantiles,
		Statuses:       make(map[int]int),
		HeaderStats:    descriptivestats.Stats{},
		HeaderQuantile: *(quantile.NewTargeted(quantiles...)),
		BodyStats:      descriptivestats.Stats{},
		BodyQuantile:   *(quantile.NewTargeted(quantiles...)),
	}
}

func (stats *Stats) Summarize() (summary StatsSummary) {
	summary.Name = stats.Name
	summary.Begin = stats.Begin
	summary.End = stats.End
	summary.Statuses = stats.Statuses
	summary.Headers.Stats = stats.HeaderStats
	summary.Headers.Quantiles = make(map[float64]float64, len(stats.Quantiles))
	for _, quantile := range stats.Quantiles {
		summary.Headers.Quantiles[quantile] = stats.HeaderQuantile.Query(quantile)
	}
	summary.Body.Stats = stats.BodyStats
	if stats.BodyStats.Count > 0 {
		summary.Body.Quantiles = make(map[float64]float64, len(stats.Quantiles))
		for _, quantile := range stats.Quantiles {
			summary.Body.Quantiles[quantile] = stats.BodyQuantile.Query(quantile)
		}
	}
	return
}

func (hammer *Hammer) collectResults() {
	defer hammer.finishedResults.Done()

	statsMap := map[string]*Stats{}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	defer func() {
		for _, stats := range statsMap {
			hammer.stats <- stats.Summarize()
		}
		close(hammer.stats)
	}()

	for {
		select {
		case res, ok := <-hammer.results:
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
			if res.GotBody != (time.Time{}) {
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
				hammer.stats <- stats.Summarize()
			}
		}
	}
}

func (stats *StatsSummary) PrintReport(w io.Writer) {
	runTime := stats.End.Sub(stats.Begin).Seconds()
	count := stats.Headers.Count
	fmt.Fprintf(
		w,
		`Run time: %.3f
Total hits: %.0f
Hits/sec: %.3f

Status totals:
`,
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
		fmt.Fprintf(w, "%d\t%d\t%.3f\n", code, stats.Statuses[code], 100*float64(stats.Statuses[code])/count)
	}
	if count > 0 {
		fmt.Fprintf(
			w,
			"\nFirst byte mean +/- SD: %.2f +/- %.2f ms\n",
			1000*stats.Headers.Mean(),
			1000*stats.Headers.StdDev(),
		)
		fmt.Fprintf(
			w,
			"First byte 5-95 pct: (%.2f, %.2f) ms\n",
			1000*stats.Headers.Quantiles[0.05],
			1000*stats.Headers.Quantiles[0.95],
		)
		if stats.Body.Count > 0 {
			fmt.Fprintf(
				w,
				"\nFull response mean +/- SD: %.2f +/- %.2f ms\n",
				1000*stats.Body.Mean(),
				1000*stats.Body.StdDev(),
			)
			fmt.Fprintf(
				w,
				"First byte 5-95 pct: (%.2f, %.2f) ms\n",
				1000*stats.Body.Quantiles[0.05],
				1000*stats.Body.Quantiles[0.95],
			)
		}
	}
}

func (hammer *Hammer) ReportPrinter(format string) func(StatsSummary) {
	return func(stats StatsSummary) {
		w, err := os.Create(fmt.Sprintf(format, stats.Name))
		if err != nil {
			hammer.warn(err.Error())
			return
		}
		fmt.Fprintf(w, "HAMMER REPORT FOR %s\n\n", stats.Name)
		stats.PrintReport(w)
		w.Close()
	}
}

func (hammer *Hammer) StatsPrinter(filename string) func(StatsSummary) {
	return func(stats StatsSummary) {
		statsFile, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			hammer.warn(err.Error())
			return
		}
		runTime := stats.End.Sub(stats.Begin).Seconds()
		count := stats.Headers.Count
		fmt.Fprintf(
			statsFile,
			"%s\t%d\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f",
			stats.Name,
			hammer.Threads,
			hammer.QPS,
			runTime,
			count,
			count/runTime,
			1000*stats.Headers.Mean(),
			1000*stats.Headers.StdDev(),
			1000*stats.Headers.Quantiles[0.05],
			1000*stats.Headers.Quantiles[0.95],
		)
		if stats.Body.Count > 0 {
			fmt.Fprintf(
				statsFile,
				"%f\t%f\t%f\t%f\n",
				1000*stats.Body.Mean(),
				1000*stats.Body.StdDev(),
				1000*stats.Body.Quantiles[0.05],
				1000*stats.Body.Quantiles[0.95],
			)
		} else {
			fmt.Fprintf(statsFile, "\n")
		}
		statsFile.Close()
	}
}

func (hammer *Hammer) Run(statschan chan StatsSummary) {
	hammer.exit = make(chan int)
	hammer.requests = make(chan Request)
	hammer.throttled = make(chan Request, hammer.Backlog)
	hammer.results = make(chan Result, hammer.Threads*2)
	hammer.stats = statschan

	for i := 0; i < hammer.Threads; i++ {
		hammer.requestWorkers.Add(1)
		go hammer.sendRequests()
	}
	hammer.finishedResults.Add(1)
	go hammer.collectResults()
	go hammer.throttle()
	go hammer.GenerateFunction(hammer)
	go func() {
		hammer.requestWorkers.Wait()
		close(hammer.results)
	}()

	// Give it time to run...
	time.Sleep(time.Duration(hammer.RunFor * float64(time.Second)))
	// And then signal GenerateRequests to stop.
	close(hammer.exit)
	hammer.finishedResults.Wait()
}
