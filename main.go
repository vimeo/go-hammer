package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/bmizerany/perks/quantile"

	"gopkg.in/yaml.v2"
)

type Config struct {
	RunFor float64 `yaml:"run_for"`
	Groups map[string]Group
}

type Group struct {
	Name      string
	Threads   int
	Backlog   int
	QPS       float64
	URLs      []string
	Headers   map[string][]string
	ReadBody  bool
	LogErrors bool
}

type Result struct {
	Status     int
	Start      time.Time
	GotHeaders time.Time
	GotBody    time.Time
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

func SendRequests(group Group, requests <-chan *http.Request, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	/*    client := &http.Client{
	      Transport: &http.Transport{
	          Proxy: http.ProxyFromEnvironment,
	          Dial: (&reuseport.Dialer{
	              D: net.Dialer{
	                  Timeout: 5 * time.Second,
	                  KeepAlive: 5 * time.Second,
	              },
	          }).Dial,
	          TLSHandshakeTimeout: 10 * time.Second,
	      },
	  }*/

	client := &http.Client{}

	for req := range requests {
		var result Result
		result.Start = time.Now()
		res, err := client.Do(req)
		result.GotHeaders = time.Now()
		if err != nil {
			result.Status = 499
			result.GotBody = result.GotHeaders
			log.Println(err)
		} else {
			result.Status = res.StatusCode
			if result.Status >= 400 {
				if group.LogErrors {
					logOut, err := ioutil.TempFile(".", "error.log.")
					if err == nil {
						res.Write(logOut)
					} else {
						log.Printf("%s writing error log", err.Error())
					}
				}
				log.Printf("Got status %s for %s\n", res.Status, req.URL.String())
			} else if group.ReadBody {
				io.Copy(ioutil.Discard, res.Body)
				result.GotBody = time.Now()
			} else {
				res.Body.Close()
			}
		}
		results <- result
	}
}

func CollectResults(group Group, results <-chan Result, wg *sync.WaitGroup) {
	defer wg.Done()

	var firstStart, lastStart time.Time
	var timeinit bool
	statuses := make(map[int]int)
	headerStats := BasicStats{}
	bodyStats := BasicStats{}
	headerQuantile := quantile.NewTargeted(0.05, 0.95)
	bodyQuantile := quantile.NewTargeted(0.05, 0.95)

	outputStats := func() {
		file, err := os.Create(fmt.Sprintf("hammer-report.%s", group.Name))
		if err != nil {
			log.Print(err)
			return
		}
		runTime := lastStart.Sub(firstStart).Seconds()
		count := headerStats.Count
		fmt.Fprintf(file, "HAMMER REPORT FOR %s:\n\n", group.Name)
		fmt.Fprintf(file, "Run time: %.3f\n", runTime)
		fmt.Fprintf(file, "Total hits: %.0f\n", count)
		fmt.Fprintf(file, "Hits/sec: %.3f\n", count/runTime)
		fmt.Fprintf(file, "\nStatus totals:\n")
		statusCodes := []int{}
		for code, _ := range statuses {
			statusCodes = append(statusCodes, code)
		}
		sort.Ints(statusCodes)
		for _, code := range statusCodes {
			fmt.Fprintf(file, "%d\t%d\n", code, statuses[code])
		}
		if count > 0 {
			fmt.Fprintf(
				file,
				"\nFirst byte mean +/- SD: %.2f +/- %.2f ms\n",
				1000*headerStats.Mean(),
				1000*headerStats.StdDev(),
			)
			fmt.Fprintf(
				file,
				"First byte 5-95 pct: (%.2f, %.2f) ms\n",
				1000*headerQuantile.Query(0.05),
				1000*headerQuantile.Query(0.95),
			)
			if group.ReadBody {
				fmt.Fprintf(
					file,
					"\nFull response mean +/- SD: %.2f +/- %.2f ms\n",
					1000*bodyStats.Mean(),
					1000*bodyStats.StdDev(),
				)
				fmt.Fprintf(
					file,
					"First byte 5-95 pct: (%.2f, %.2f) ms\n",
					1000*bodyQuantile.Query(0.05),
					1000*bodyQuantile.Query(0.95),
				)
			}
		}
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	defer outputStats()
	for {
		select {
		case res, ok := <-results:
			if !ok {
				return
			}
			statuses[res.Status]++

			dur := res.GotHeaders.Sub(res.Start).Seconds()
			headerStats.Add(dur)
			headerQuantile.Insert(dur)
			if group.ReadBody {
				dur := res.GotBody.Sub(res.Start).Seconds()
				bodyStats.Add(dur)
				bodyQuantile.Insert(dur)
			}
			start := res.Start
			end := res.GotHeaders
			if group.ReadBody {
				end = res.GotBody
			}
			if !timeinit {
				firstStart = start
				lastStart = end
				timeinit = true
			} else {
				if start.Before(firstStart) {
					firstStart = start
				}
				if start.After(lastStart) {
					lastStart = start
				}
			}
		case <-ticker.C:
			outputStats()
		}
	}
}

func GenerateRequests(group Group, requests chan<- *http.Request, exit <-chan int) {
	readiedRequests := make([]*http.Request, len(group.URLs))
	for i, url := range group.URLs {
		var err error
		readiedRequests[i], err = http.NewRequest("GET", url, nil)
		if err != nil {
			panic(err)
		}
		readiedRequests[i].Header = group.Headers
	}

	ticker := time.NewTicker(time.Duration(float64(time.Second) / group.QPS))

	defer func() { close(requests) }()
	defer ticker.Stop()

	for {
		select {
		case <-exit:
			return
		case <-ticker.C:
			req := readiedRequests[rand.Intn(len(readiedRequests))]
			requests <- req
		}
	}
}

func Run(config Config) {
	exit := make(chan int)
	var finishedResults sync.WaitGroup

	for groupName, group := range config.Groups {
		group.Name = groupName
		requests := make(chan *http.Request, group.Backlog)
		results := make(chan Result, group.Threads*2)
		var groupThreads sync.WaitGroup
		for i := 0; i < group.Threads; i++ {
			groupThreads.Add(1)
			go SendRequests(group, requests, results, &groupThreads)
		}
		finishedResults.Add(1)
		go CollectResults(group, results, &finishedResults)
		go GenerateRequests(group, requests, exit)
		// When all SendRequests exit, close results so that CollectResults will exit.
		go func() {
			groupThreads.Wait()
			close(results)
		}()
	}

	// Give it time to run...
	time.Sleep(time.Duration(config.RunFor * float64(time.Second)))
	// And then signal GenerateRequests to stop.
	close(exit)
	finishedResults.Wait()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	config, _ := LoadConfig()
	Run(config)
}
