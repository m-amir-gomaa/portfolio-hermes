package httpclient

import (
	"net"
	"net/http"
	"time"
)

// NewResilientClient returns an http.Client optimized for high-concurrency LLM/API requests.
// It explicitly configures the Transport to prevent TCP port exhaustion (a classic CSAPP problem).
func NewResilientClient() *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100, // Important for worker pools
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   100, // Crucial: prevents port exhaustion when hitting the same API repeatedly
	}

	return &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second, // Hard timeout to prevent hanging goroutines
	}
}
