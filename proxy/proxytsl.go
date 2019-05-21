package proxy

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Tsl struct
type ProxyTSL struct {
	ReqCounter  prometheus.Counter
	ErrCounter  prometheus.Counter
	WarnCounter prometheus.Counter
}

// NewProxyTSL is creating a new tsl proxy query handler
func NewProxyTSL(promRegistry *prometheus.Registry) *ProxyTSL {
	proxyTsl := &ProxyTSL{}

	// metrics
	proxyTsl.ReqCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "tsl",
		Subsystem: "controller",
		Name:      "requests",
		Help:      "Number of request handled.",
	})
	promRegistry.MustRegister(proxyTsl.ReqCounter)
	proxyTsl.ErrCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "tsl",
		Subsystem: "controller",
		Name:      "errors",
		Help:      "Number of request in errors.",
	})
	promRegistry.MustRegister(proxyTsl.ErrCounter)
	proxyTsl.WarnCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "tsl",
		Subsystem: "controller",
		Name:      "warnings",
		Help:      "Number of errored client requests.",
	})
	promRegistry.MustRegister(proxyTsl.WarnCounter)

	return proxyTsl
}
