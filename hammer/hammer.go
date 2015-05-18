/*
Hammer is a utilility for load-testing HTTP servers. It attempts to combine
high performance with flexibility. It is available as a CLI tool, "hammer",
and as a library for more advanced tasks. Read on for description of the
library.

WARNING

The library interface is subject to change as the design is refined.
*/
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
	"strings"
	"sync"
	"time"

	"github.com/arodland/go-descriptivestats/descriptivestats"
	"github.com/bmizerany/perks/quantile"
)

// Hammer is the main type that coordinates several goroutines to send
// requests and collect results.
type Hammer struct {
	// Runtime in seconds. TODO: maximum number of requests instead.
	RunFor float64
	// Number of goroutines to send requests / maximum number of parallel
	// requests.
	Threads int
	// The backlog is a queue of requests between the request rate throttler
	// and the requesting threads, ready to be sent immediately. If the
	// requesting threads are unable to send requests at the desired rate,
	// the backlog will fill up. Later, if possible, requests will be sent
	// as fast as possible until the backlog is empty. The backlog can help
	// smooth over network or server hiccups, but too large a backlog can
	// result in large request spikes.
	Backlog int
	// The number of requests per second to generate.
	QPS float64
	// Whether to log all error responses received from the server to file.
	LogErrors bool
	// Request generation function
	GenerateFunction RequestGenerator
	// close(hammer.Exit) to signal the request generator to end.
	Exit            chan int
	requests        chan Request
	throttled       chan Request
	results         chan Result
	stats           chan StatsSummary
	requestWorkers  sync.WaitGroup
	finishedResults sync.WaitGroup
}

func (hammer *Hammer) warn(msg string) {
	log.Println(msg)
}

func (hammer *Hammer) warnf(fmt string, args ...interface{}) {
	log.Printf(fmt, args...)
}

type BodyBehavior int

const (
	// Don't read the response body at all.
	IgnoreBody BodyBehavior = iota
	// Read the response body and time it, but discard it.
	DiscardBody
	// Read the response body and replace it with a bytes.Reader so that it
	// can be read again by a callback.
	CopyBody
)

type Request struct {
	// HTTP request to be sent.
	HTTPRequest *http.Request
	// Name will end up in the eventual Result and Stats objects, and report
	// output.
	Name string
	// How to dispose of the response body.
	BodyBehavior BodyBehavior
	// If non-nil, will be called with the result of this request.
	Callback RequestCallback
}

// RequestCallback is called as the result of a successful Request. It
// receives the original request, an http.Response (with the Body replaced
// by a bytes.Reader if the Request had CopyBody), and the Result containing
// timings. A RequestCallback may call SendRequest or SendRequestImmediately
// to generate more requests.
type RequestCallback func(Request, *http.Response, Result)

// RequestGenerators should generate and send requests on the provided
// channel, and they should exit if the Hammer's Exit channel is closed.
type RequestGenerator func(*Hammer, chan<- Request)

type RandomURLGeneratorOptions struct {
	// URLs to send requests to.
	URLs []string
	// Headers to be added to all outgoing requests.
	Headers map[string][]string
	// The name to be used for all outgoing requests.
	Name string
	// BodyBehavior for all outgoing requests.
	BodyBehavior BodyBehavior
	// Callback for all outgoing requests.
	Callback RequestCallback
}

// RandomURLGenerator returns a RequestGenerator that will send GET requests
// to all of the URLs in a list, in random order. It's suitable for use to
// hit a single URL as well, and in that case it won't consume any random
// numbers.
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

	return func(hammer *Hammer, requests chan<- Request) {
		defer func() { close(requests) }()

		for {
			select {
			case <-hammer.Exit:
				return
			default:
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

// TODO: what we're really looking for here is probably more like separate
// rate limiters than one big rate limiter that can be bypassed. That way we
// could limit *both* the rate of primary requests and subsidiary requests
// in that scenario, instead of counting on limiting one to limit the other.

// Send a Request and record the statistics for it. The Request will be
// counted for rate-limiting purposes and will be sent when the rate-limiter
// allows.
func (hammer *Hammer) SendRequest(req Request) {
	hammer.requests <- req
}

// Send a Request and record the statistics for it. The Request will be sent
// immediately and will not count for rate-limiting purposes.
func (hammer *Hammer) SendRequestImmediately(req Request) {
	hammer.throttled <- req
}

func (hammer *Hammer) throttle() {
	ticker := time.NewTicker(time.Duration(float64(time.Second) / hammer.QPS))
	defer ticker.Stop()
	defer close(hammer.throttled)

	for {
		select {
		case <-ticker.C:
			req, ok := <-hammer.requests
			if !ok {
				return
			}
			hammer.throttled <- req
		}
	}
}

type Result struct {
	Name string
	// HTTP status code.
	Status int
	// Time at the beginning of the request.
	Start time.Time
	// Time after receiving the response headers, before receiving the body.
	GotHeaders time.Time
	// Time after receiving the entire response body.
	GotBody time.Time
	// Body size in bytes.
	BodySize int64
}

func copyResponseBody(res *http.Response) (gotBody time.Time, size int64) {
	var buf bytes.Buffer
	size, _ = io.Copy(&buf, res.Body)
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
					result.GotBody, result.BodySize = copyResponseBody(res)
				}
				if hammer.LogErrors {
					// TODO: refactor this into a method
					logOut, err := ioutil.TempFile(".", "error.log.")
					if err == nil {
						defer logOut.Close()
						req.HTTPRequest.Write(logOut)
						res.Write(logOut)
						result.GotBody = time.Now()
					} else {
						hammer.warnf("%s writing error log\n", err.Error())
					}
				} else if req.BodyBehavior == DiscardBody {
					result.BodySize, _ = io.Copy(ioutil.Discard, res.Body)
					result.GotBody = time.Now()
				}
				hammer.warnf("Got status %s for %s\n", res.Status, req.HTTPRequest.URL.String())
			} else if req.BodyBehavior == CopyBody {
				result.GotBody, result.BodySize = copyResponseBody(res)
			} else if req.BodyBehavior == DiscardBody {
				result.BodySize, _ = io.Copy(ioutil.Discard, res.Body)
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
	Bytes          float64
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
	Bytes    float64
}

func newStats(name string, quantiles ...float64) *Stats {
	stats := Stats{
		Name:           name,
		Quantiles:      quantiles,
		Statuses:       make(map[int]int),
		HeaderStats:    descriptivestats.Stats{},
		HeaderQuantile: *(quantile.NewTargeted(quantiles...)),
		BodyStats:      descriptivestats.Stats{},
		BodyQuantile:   *(quantile.NewTargeted(quantiles...)),
	}
	stats.HeaderQuantile.SetEpsilon(0.001)
	stats.BodyQuantile.SetEpsilon(0.001)
	return &stats
}

func (stats *Stats) Summarize() (summary StatsSummary) {
	summary.Name = stats.Name
	summary.Begin = stats.Begin
	summary.End = stats.End
	summary.Statuses = stats.Statuses
	summary.Bytes = stats.Bytes
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
				stats = newStats(res.Name, 0.05, 0.5, 0.95)
				statsMap[res.Name] = stats
			}

			stats.Statuses[res.Status]++
			stats.Bytes += float64(res.BodySize)

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
`,
		runTime,
		count,
		count/runTime,
	)
	if stats.Body.Count > 0 {
		fmt.Fprintf(
			w,
			"Total bytes: %0.f\nBytes/sec: %.0f\n",
			stats.Bytes,
			stats.Bytes/runTime,
		)
	}

	fmt.Fprintf(w, "\nStatus totals:\n")
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
			"\nFirst byte mean +/- std. dev.: %.2f +/- %.2f ms\n",
			1000*stats.Headers.Mean(),
			1000*stats.Headers.StdDev(),
		)
		fmt.Fprintf(
			w,
			"First byte quantiles: (%.2f, %.2f, %.2f) ms\n",
			1000*stats.Headers.Quantiles[0.05],
			1000*stats.Headers.Quantiles[0.5],
			1000*stats.Headers.Quantiles[0.95],
		)
		if stats.Body.Count > 0 {
			fmt.Fprintf(
				w,
				"\nFull response mean +/- std. dev.: %.2f +/- %.2f ms\n",
				1000*stats.Body.Mean(),
				1000*stats.Body.StdDev(),
			)
			fmt.Fprintf(
				w,
				"Full response quantiles: (%.2f, %.2f, %.2f) ms\n",
				1000*stats.Body.Quantiles[0.05],
				1000*stats.Body.Quantiles[0.5],
				1000*stats.Body.Quantiles[0.95],
			)
		}
	}
}

func qualifyFilename(format string, stats StatsSummary) string {
	if !strings.Contains(format, "*") {
		return format
	}
	format = strings.Replace(format, "%", "%%", -1)
	format = strings.Replace(format, "*", "%s", 1)
	return fmt.Sprintf(format, stats.Name)
}

func (hammer *Hammer) ReportPrinter(format string) func(StatsSummary) {
	return func(stats StatsSummary) {
		w, err := os.Create(qualifyFilename(format, stats))
		if err != nil {
			hammer.warn(err.Error())
			return
		}
		defer w.Close()
		fmt.Fprintf(w, "HAMMER REPORT FOR %s\n\n", stats.Name)
		stats.PrintReport(w)
	}
}

func (hammer *Hammer) StatsPrinter(format string) func(StatsSummary) {
	return func(stats StatsSummary) {
		statsFile, err := os.OpenFile(
			qualifyFilename(format, stats),
			os.O_WRONLY|os.O_CREATE|os.O_APPEND,
			0666,
		)
		if err != nil {
			hammer.warn(err.Error())
			return
		}
		defer statsFile.Close()
		runTime := stats.End.Sub(stats.Begin).Seconds()
		count := stats.Headers.Count
		fmt.Fprintf(
			statsFile,
			"%s\t%d\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f",
			stats.Name,
			hammer.Threads,
			hammer.QPS,
			runTime,
			count,
			count/runTime,
			1000*stats.Headers.Mean(),
			1000*stats.Headers.StdDev(),
			1000*stats.Headers.Quantiles[0.05],
			1000*stats.Headers.Quantiles[0.5],
			1000*stats.Headers.Quantiles[0.95],
		)
		if stats.Body.Count > 0 {
			fmt.Fprintf(
				statsFile,
				"%f\t%f\t%f\t%f\t%f\t%.0f\t%.0f\n",
				1000*stats.Body.Mean(),
				1000*stats.Body.StdDev(),
				1000*stats.Body.Quantiles[0.05],
				1000*stats.Body.Quantiles[0.5],
				1000*stats.Body.Quantiles[0.95],
				stats.Bytes,
				stats.Bytes/runTime,
			)
		} else {
			fmt.Fprintf(statsFile, "\n")
		}
	}
}

// Run will set a Hammer in motion, producing StatsSummary values on the
// provided channel. Closes the channel after all responses are received,
// all stats are tabulated, and a final StatsSummary is sent out.
func (hammer *Hammer) Run(statschan chan StatsSummary) {
	if hammer.Backlog == 0 {
		hammer.Backlog = hammer.Threads
	}

	hammer.Exit = make(chan int)
	hammer.requests = make(chan Request)
	hammer.results = make(chan Result, hammer.Threads*2)
	hammer.stats = statschan

	if hammer.QPS > 0 {
		hammer.throttled = make(chan Request, hammer.Backlog)
		go hammer.throttle()
	} else {
		hammer.throttled = hammer.requests
	}

	for i := 0; i < hammer.Threads; i++ {
		hammer.requestWorkers.Add(1)
		go hammer.sendRequests()
	}
	hammer.finishedResults.Add(1)
	go hammer.collectResults()
	go hammer.GenerateFunction(hammer, hammer.requests)
	go func() {
		hammer.requestWorkers.Wait()
		close(hammer.results)
	}()

	// Give it time to run...
	time.Sleep(time.Duration(hammer.RunFor * float64(time.Second)))
	// And then signal GenerateRequests to stop.
	close(hammer.Exit)
	hammer.finishedResults.Wait()
}
