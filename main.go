package main

import (
	"io/ioutil"
	"os"
	"runtime"
	"sync"

	"github.com/arodland/go-hammer/hammer"
	"gopkg.in/yaml.v2"
)

type Config struct {
	RunFor float64 `yaml:"run_for"`
	Groups map[string]Group
}

type Group struct {
	Threads   int
	Backlog   int
	QPS       float64
	URLs      []string
	Headers   map[string][]string
	ReadBody  bool
	LogErrors bool
}

func LoadConfig() (config Config, err error) {
	configFile, err := os.Open("hammer.yaml")
	if err != nil {
		return
	}
	configYaml, err := ioutil.ReadAll(configFile)
	if err != nil {
		return
	}
	err = yaml.Unmarshal(configYaml, &config)
	return
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	config, _ := LoadConfig()
	var wg sync.WaitGroup

	for groupName, group := range config.Groups {
		h := hammer.Hammer{
			RunFor:           config.RunFor,
			Threads:          group.Threads,
			Backlog:          group.Backlog,
			QPS:              group.QPS,
			ReadBody:         group.ReadBody,
			LogErrors:        group.LogErrors,
			GenerateFunction: hammer.RandomURLGenerator(groupName, group.URLs, group.Headers),
		}
		statschan := make(chan hammer.StatsSummary)
		printReport := h.ReportPrinter("hammer-report.%s")
		printStats := h.StatsPrinter("stats")

		wg.Add(1)
		go func() {
			defer wg.Done()
			var stats hammer.StatsSummary
			for stats = range statschan {
				printReport(stats)
			}
			printReport(stats)
			printStats(stats)
		}()

		go h.Run(statschan)
	}
	wg.Wait()
}
