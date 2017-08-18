package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
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
	http.HandleFunc("/query", QueryHandler(db))
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

// Query is the query JSON holder.
type Query struct {
	Promql  string `json:"promql"`
	MinTime int64  `json:"mint"`
	MaxTime int64  `json:"maxt"`
}

type response struct {
	Series []series `json:"series"`
}

type series struct {
	Labels labels.Labels `json:"labels"`
	Points []point       `json:"points"`
}

type point struct {
	T int64   `json:"t"`
	V float64 `json:"v"`
}

// QueryHandler returns the handler for Queries.
func QueryHandler(db *tsdb.DB) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		var q Query
		if err := decoder.Decode(&q); err != nil {
			panic(err)
		}
		defer r.Body.Close()

		matchedSer := make([]series, 0)

		querier := db.Querier(q.MinTime, q.MaxTime)
		defer querier.Close()
		matchers, err := PromQLToMatchers(q.Promql)
		if err != nil {
			panic(err)
		}

		ss := querier.Select(matchers...)

		for ss.Next() {
			s := ss.At()
			labels := s.Labels()

			it := s.Iterator()

			pts := make([]point, 0)
			for it.Next() {
				t, v := it.At()
				pts = append(pts, point{t, v})
			}

			matchedSer = append(matchedSer, series{labels, pts})
		}

		json.NewEncoder(w).Encode(response{Series: matchedSer})
	}
}
