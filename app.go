package main

import (
	. "diff-redis/src"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
)

var (
	app            = kingpin.New("diff-redis", "Find out the missing keys")
	source         = app.Flag("source", "Source Redis. host:port").Required().Short('s').String()
	sourcePassword = app.Flag("source-password", "Password of source redis").Short('p').String()
	target         = app.Flag("target", "target Redis. host:port").Required().Short('t').String()
	targetPassword = app.Flag("target-password", "Password of target redis").Short('a').String()
	output         = app.Flag("output", "Write result to").Default("diff-redis.result").Short('o').String()
	count          = app.Flag("count", "Count of per scan").Default("256").Int()
)

func main() {
	app.HelpFlag.Short('h')

	if _, err := app.Parse(os.Args[1:]); err != nil {
		app.FatalUsage("%s\r\n", err)
	}

	config := Config{
		Source: *source, SourcePassword: *sourcePassword,
		Target: *target, TargetPassword: *targetPassword,
		Output: *output,
		Count:  *count,
	}

	Diff(&config)
}
