# Hammer

Hammer is an HTTP load-testing / load-generation tool. Its goals are to be
high-performance and flexible. It consists of a CLI tool similar to
apachebench (ab) or siege, and a library that can be used for more advanced
scenarios.

## Installing the CLI tool

    go get github.com/vimeo/go-hammer/cli/hammer

## Using the CLI tool

Please read the
[documentation](http://godoc.org/github.com/vimeo/go-hammer/cli/hammer).

## Installing the library

    go get github.com/vimeo/go-hammer/hammer

## Example library use

    import "github.com/vimeo/go-hammer/hammer"

    h := hammer.Hammer{
        RunFor: 30,
        Threads: 10,
        QPS: 100,
        GenerateFunction: hammer.RandomURLGenerator(
            hammer.RandomURLGeneratorOptions{
                URLs: []string{"http://www.example.com"},
                Name: "example"
            },
        ),
    }

    statschan := make(chan hammer.StatsSummary)

    go func() {
        for stats := range statschan {
            stats.PrintReport(os.Stdout)
        }
    }

    h.Run(statschan)

## Library documentation

[GoDoc](http://godoc.org/github.com/vimeo/go-hammer/hammer).
