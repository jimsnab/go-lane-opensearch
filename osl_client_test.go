package osl

import (
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
)

func TestBulkInsert(t *testing.T) {
	tl := lane.NewTestingLane(nil)

	// start the stub server
	stub := newStubServer(t, nil)
	defer stub.Close()

	// configure the OpenSearch client to point to the stub server
	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{stub.URL()},
	})
	if err != nil {
		t.Fatalf("failed to create OpenSearch client: %v", err)
	}

	// create a bulk request
	bulkReq := opensearchapi.BulkRequest{
		Body: strings.NewReader(`{"index":{}}
{"field1":"value1"}`),
	}

	// execute the bulk request
	resp, err := bulkReq.Do(tl, client)
	if err != nil {
		t.Fatalf("failed to execute bulk request: %v", err)
	}
	defer resp.Body.Close()

	// verify the response
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status code 200, got %v", resp.StatusCode)
	}
}

func TestOslBulkInsert(t *testing.T) {
	// start the stub server
	var wg sync.WaitGroup
	stub := newStubServer(t, &wg)
	defer stub.Close()

	protocol, host, port := stub.Connection()

	// create an opensearch lane
	cfg := OslConfig{
		OpenSearchProtocol:  protocol,
		OpenSearchHost:      host,
		OpenSearchPort:      port,
		OpenSearchTransport: stub.NewTransport(),
		OpenSearchIndex:     "sample",
		LogThreshold:        10,
		MaxBufferSize:       10,
		BackoffInterval:     time.Millisecond,
		BackoffLimit:        time.Millisecond * 10,
	}
	osl, err := NewOpenSearchLane(nil, &cfg)
	if err != nil {
		t.Fatal(err)
	}

	wg.Add(1)
	osl.Info(500)

	wg.Wait()

	for range 40 {
		stats := osl.Stats()
		if stats.MessagesSent == 1 {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
}
