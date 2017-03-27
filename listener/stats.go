package listener

import (
	"time"
)

// This was really built with rcrowley/go-metrics in mind.
type StatsListener interface {
	// histograms
	AddConsumedSample(int)
	AddDeliveredSample(int)
	AddProcessedSample(int)
	AddBatchSizeSample(int)

	// meters
	AddGetRecordsCalled(int)
	AddProvisionedThroughputExceeded(int)
	AddGetRecordsTimeout(int)
	AddGetRecordsReadTimeout(int)

	// timers
	AddGetRecordsReadResponseTime(time.Duration)
	AddGetRecordsUnmarshalTime(time.Duration)
}

type NilStatsListener struct{}

func (l *NilStatsListener) AddConsumedSample(int)                       {}
func (l *NilStatsListener) AddDeliveredSample(int)                      {}
func (l *NilStatsListener) AddProcessedSample(int)                      {}
func (l *NilStatsListener) AddBatchSizeSample(int)                      {}
func (l *NilStatsListener) AddGetRecordsCalled(int)                     {}
func (l *NilStatsListener) AddProvisionedThroughputExceeded(int)        {}
func (l *NilStatsListener) AddGetRecordsTimeout(int)                    {}
func (l *NilStatsListener) AddGetRecordsReadTimeout(int)                {}
func (l *NilStatsListener) AddGetRecordsReadResponseTime(time.Duration) {}
func (l *NilStatsListener) AddGetRecordsUnmarshalTime(time.Duration)    {}
