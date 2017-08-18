package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
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

	// HTTP server stuff.
	http.HandleFunc("/insert", InsertHandler(db))
	http.ListenAndServe(":8080", nil)
}

// InsertHandler returns the insert handler.
func InsertHandler(db *tsdb.DB) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		buf, err := ioutil.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		defer r.Body.Close()

		metrics, err := LineToMetrics(buf)
		if err != nil {
			panic(err)
		}

		// Add logic.
		app := db.Appender()
		for _, metric := range metrics {
			ref, err := app.Add(metric.Series, metric.Timestamp, metric.Value)
			if err != nil {
				panic(err)
			}
			// ref can be used to do app.AddFast(ref, ts, val) but here we just don't use it.
			_ = ref
		}

		if err := app.Commit(); err != nil {
			panic(err)
		}
		// We can also do app.Rollback() which would just drop everything to the floor.

		fmt.Fprintln(w, "Success")
	}
}
