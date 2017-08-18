package main

import (
	"sort"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/prometheus/tsdb/labels"
)

// Metric is one point on a time-series.
type Metric struct {
	Series labels.Labels

	Timestamp int64
	Value     float64
}

// LineToMetrics converts line protocol to tsdb Metrics.
func LineToMetrics(buf []byte) ([]Metric, error) {
	pts, err := models.ParsePointsWithPrecision(buf, time.Now(), "ms")
	if err != nil {
		return nil, err
	}

	mets := make([]Metric, 0, len(pts))
	for _, pt := range pts {
		tags := pt.Tags()
		series := make(labels.Labels, 0, len(tags))
		for _, tag := range tags {
			series = append(series, labels.Label{Name: string(tag.Key), Value: string(tag.Value)})
		}
		series = append(series, labels.Label{Name: "name", Value: pt.Name()})
		sort.Sort(series)

		var val float64
		fIt := pt.FieldIterator()
		for fIt.Next() {
			if fIt.Type() == models.Integer {
				tval := fIt.IntegerValue()
				val = float64(tval)
			}

			if fIt.Type() == models.Float {
				tval := fIt.FloatValue()
				val = tval
			}
		}

		mets = append(mets, Metric{
			Series:    series,
			Timestamp: int64(pt.Time().Unix()),
			Value:     val,
		})
	}

	return mets, nil
}
