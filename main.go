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
var backlog = goopt.Int([]string{"-b", "--backlog"}, 10, "request backlog size")
var qps = goopt.Int([]string{"-r", "--rate"}, 10, "rumber of requests/second to send")
var urls = goopt.Strings([]string{"-u", "--url"}, "URL", "specify URLs to request")
var headerOpt = goopt.Strings([]string{"-h", "--header"}, "header", "specify headers to send with requests")

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

var name = goopt.String([]string{"--name"}, "hammer", "name for reporting")
var report = goopt.String([]string{"--report"}, "", "filename to write a report")
var statsfile = goopt.String([]string{"--stats"}, "", "filename to write statistics")

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
