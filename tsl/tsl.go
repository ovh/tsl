package tsl

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Tsl struct
type Tsl struct {
	ReqCounter  prometheus.Counter
	ErrCounter  prometheus.Counter
	WarnCounter prometheus.Counter
}

// ProtoParser contains proto global data
type ProtoParser struct {
	lineStart int    // Reset line counter to lineStart
	name      string // Proto name

}

// NewTsl is creating a new tsl query handler
func NewTsl() *Tsl {
	tsl := &Tsl{}

	// metrics
	tsl.ReqCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "tsl",
		Subsystem: "controller",
		Name:      "requests",
		Help:      "Number of request handled.",
	})
	prometheus.MustRegister(tsl.ReqCounter)
	tsl.ErrCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "tsl",
		Subsystem: "controller",
		Name:      "errors",
		Help:      "Number of request in errors.",
	})
	prometheus.MustRegister(tsl.ErrCounter)
	tsl.WarnCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "tsl",
		Subsystem: "controller",
		Name:      "warnings",
		Help:      "Number of errored client requests.",
	})
	prometheus.MustRegister(tsl.WarnCounter)

	return tsl
}
