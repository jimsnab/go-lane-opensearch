package osl

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type stubServer struct {
	server   *httptest.Server
	t        *testing.T
	wg       *sync.WaitGroup
	Force401 bool
}

func newStubServer(t *testing.T, wg *sync.WaitGroup) *stubServer {
	s := &stubServer{t: t, wg: wg}

	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// handle the GET / request for cluster info
		if r.Method == http.MethodGet && r.URL.Path == "/" {
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintln(w, `{"cluster_name":"test-cluster","version":{"number":"7.10.2"}}`)
			return
		}

		// handle the _bulk requests
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "_bulk") {
			if s.Force401 {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("failed to read request body: %v", err)
				http.Error(w, "failed to read request body", http.StatusInternalServerError)
				return
			}
			defer r.Body.Close()

			// validate the request body
			if len(body) == 0 {
				t.Errorf("request body is empty")
				http.Error(w, "empty body", http.StatusBadRequest)
				return
			}

			// respond with 200 OK to indicate success
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintln(w, `{"took": 1, "errors": false}`)

			if s.wg != nil {
				s.wg.Done()
			}
			return
		}

		// unexpected request
		t.Errorf("unexpected request: method=%v path=%v", r.Method, r.URL.Path)
		http.Error(w, "invalid request", http.StatusBadRequest)
	}))

	return s
}

func (s *stubServer) URL() string {
	return s.server.URL
}

func (s *stubServer) Connection() (protocol, host string, port int) {
	parsedURL, err := url.Parse(s.server.URL)
	if err != nil {
		panic(fmt.Sprintf("failed to parse URL: %v", err))
	}

	protocol = parsedURL.Scheme

	// split the host and port
	hostPort := parsedURL.Host
	if strings.Contains(hostPort, ":") {
		parts := strings.Split(hostPort, ":")
		host = parts[0]
		port, err = strconv.Atoi(parts[1])
		if err != nil {
			panic(err)
		}
	} else {
		// default to the HTTP port if no port is specified
		host = hostPort
		port = 80
		if protocol == "https" {
			port = 443
		}
	}

	return
}

// NewTransport creates an *http.Transport instance that can connect to the stub server.
func (s *stubServer) NewTransport() *http.Transport {
	protocol, host, port := s.Connection()

	// configure transport
	return &http.Transport{
		TLSClientConfig: &tls.Config{
			// Accept all certificates for HTTPS (for test purposes only)
			InsecureSkipVerify: protocol == "https",
		},
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			// Redirect all requests to the stub server
			if addr == host || addr == fmt.Sprintf("%s:80", host) || addr == fmt.Sprintf("%s:443", host) {
				addr = fmt.Sprintf("%s:%d", host, port)
			}
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	}
}

func (s *stubServer) Close() {
	s.server.Close()
}
