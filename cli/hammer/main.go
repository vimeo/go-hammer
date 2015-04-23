/*
Hammer is a utility for load-testing HTTP servers. In command-line form you
can think of it as an alternative to apachebench or siege, sending requests
at a controlled rate to a given URL or list of URLs and reporting on
throughput, latency, and the status codes of the responses. It is also
structured as a library that can be used to perform more advanced tasks,
such as using the response to one request to create a later request, and
gathering statistics for each type of request separately.

Example

hammer --threads 10 -r 20 -t 60 http://www.example.com/

Will send 20 requests per second to http://www.example.com/ for 60 seconds,
making a maximum of 10 concurrent requests.

Command Line Options

--url, -u: URLs to request

Specify one or more URLs for hammer to send requests to. Any options
provided on the commandline without a flag will also be interpreted as URLs,
so the use of --url or -u is optional.

--threads, -T: Number of request goroutines

This value controls the number of goroutines that will be used to send
requests to servers, which limits the maximum number of parallel requests
that can be in flight at one time. Too low a value can result in fewer
requests being sent than you requested; too high can result in resource
exhaustion on the target server or the hammering machine itself.

--time, --run-for, -t: Runtime (seconds)

Hammer will run until this timer expires, or until it is terminated by a
signal.

--rate, -r: Request rate (requests/second)

Hammer will attempt to send this many requests per second in total. If you
specify multiple URLs, the requests will be divided up evenly among all of
them.

--backlog, -b: Backlog size (requests)

The backlog is a queue of requests between the request rate throttler and
the requesting threads, ready to be sent immediately. If the requesting
threads are unable to send requests at the desired rate, the backlog will
fill up. Later, if possible, requests will be sent as fast as possible until
the backlog is empty in an attempt to "catch up". The backlog can help
smooth over network or server hiccups, but too large a backlog can result in
large request spikes.

--no-read-body, --read-body:

By default, hammer will read the entire HTTP response body and record the
time that it took to receive the response. With --no-read-body, hammer will
only read until the end of the headers, and no body timings will be
available. In the case of large or slow response bodies, you may be able to
generate many more requests by ignoring the bodies.

--log-errors, --no-log-errors:

If --log-errors is enabled, hammer will write a file to disk containing the
response headers and body whenever a 4xx or 5xx HTTP response is received
from the server. This may help to diagnose transient problems that appear
under load.

--report: Report filename

If this option is set, it specifies a file to write statistics to every
second during the run, in addition to writing them to the console at the end
of the run. If the filename contains a '*' character it will be replaced
with the value of the --name option.

--stats: Stats filename

If this option is set, it specifies a file to append abbreviated statistics
(single-line, tab-separated) to at the end of the run.

--name: Stats name

Name to use for --report and --stats output.

--header, -h:

Use --header to add headers to outgoing requests. Use as --header 'Name:
value'. Can be specified multiple times.
*/
package main

import (
	"fmt"
	"os"
	"regexp"
	"runtime"
	"sync"

	"github.com/arodland/go-hammer/hammer"
	"github.com/droundy/goopt"
)

var threads = goopt.Int([]string{"-T", "--threads"}, 4, "number of request threads")
var runFor = goopt.Int([]string{"-t", "--time", "--run-for"}, 60, "runtime in seconds")
var qps = goopt.Int([]string{"-r", "--rate"}, 10, "rumber of requests/second to send")
var backlog = goopt.Int([]string{"-b", "--backlog"}, 4, "request backlog size")

var skipBody = goopt.Flag(
	[]string{"--no-read-body"},
	[]string{"--read-body"},
	"Close the connection without reading the request body; don't measure the time to read the body.",
	"Time the request body.",
)

var logErrors = goopt.Flag(
	[]string{"--log-errors"},
	[]string{"--no-log-errors"},
	"Log all server error responses to disk.",
	"Don't log error responses.",
)

var report = goopt.String([]string{"--report"}, "", "filename to write a report")
var statsfile = goopt.String([]string{"--stats"}, "", "filename to write statistics")
var name = goopt.String([]string{"--name"}, "hammer", "name for reporting")

var headerOpt = goopt.Strings([]string{"-h", "--header"}, "header", "specify headers to send with requests")
var urls = goopt.Strings([]string{"-u", "--url"}, "URL", "specify URLs to request")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	goopt.Parse(nil)

	*urls = append(*urls, goopt.Args...)

	if len(*urls) == 0 {
		fmt.Print(goopt.Help())
		return
	}

	var headers map[string][]string

	colon := regexp.MustCompile(`\s*:\s*`)
	for _, header := range *headerOpt {
		parts := colon.Split(header, 2)
		if headers[parts[0]] == nil {
			headers[parts[0]] = []string{parts[1]}
		} else {
			headers[parts[0]] = append(headers[parts[0]], parts[1])
		}
	}

	opts := hammer.RandomURLGeneratorOptions{
		URLs:    *urls,
		Headers: headers,
		Name:    *name,
	}
	if *skipBody {
		opts.BodyBehavior = hammer.IgnoreBody
	} else {
		opts.BodyBehavior = hammer.DiscardBody
	}
	generator := hammer.RandomURLGenerator(opts)

	h := hammer.Hammer{
		RunFor:           float64(*runFor),
		Threads:          *threads,
		Backlog:          *backlog,
		QPS:              float64(*qps),
		LogErrors:        *logErrors,
		GenerateFunction: generator,
	}

	var reportPrinter func(hammer.StatsSummary)
	if *report != "" {
		reportPrinter = h.ReportPrinter(*report)
	}
	var statsPrinter func(hammer.StatsSummary)
	if *statsfile != "" {
		statsPrinter = h.StatsPrinter(*statsfile)
	}

	statschan := make(chan hammer.StatsSummary)
	wg := sync.WaitGroup{}
	wg.Add(1)
	var percentDone int

	go func() {
		var stats hammer.StatsSummary
		for stats = range statschan {
			if reportPrinter != nil {
				reportPrinter(stats)
			}
			runTime := stats.End.Sub(stats.Begin).Seconds()
			percent := int(100 * runTime / h.RunFor)
			if percent/10 > percentDone/10 && percent < 100 {
				fmt.Printf("%d%%...", percent)
			}
			percentDone = percent
		}
		fmt.Printf("done\n")
		stats.PrintReport(os.Stdout)
		if statsPrinter != nil {
			statsPrinter(stats)
		}
		wg.Done()
	}()

	h.Run(statschan)
	wg.Wait()
}
