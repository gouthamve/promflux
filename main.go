package main

import (
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb"
)

func main() {
	// Create the TSDB instance.
	db, err := tsdb.Open(
		"./data",                       // The path to the directory where all the data will be stored.
		log.NewLogfmtLogger(os.Stdout), // The logger instance.
		prometheus.NewRegistry(),       // The prometheus registry.
		&tsdb.Options{
			WALFlushInterval:  10 * time.Second,
			RetentionDuration: 15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds.
			BlockRanges: []int64{ // The sizes of the blocks. We have a helper to generate the sizes.
				2 * 60 * 60 * 1000,  // 2hrs
				6 * 60 * 60 * 1000,  // 6hrs
				24 * 60 * 60 * 1000, // 24hrs
				72 * 60 * 60 * 1000, // 72 hrs
			},
		},
	)
	if err != nil {
		panic(err)
	}
}
