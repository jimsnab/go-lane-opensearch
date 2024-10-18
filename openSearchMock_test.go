package osl

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

type (
	testClient struct {
		orgNewClient func(openSearchHost string, openSearchPort int, openSearchUser, openSearchPass string, openSearchTransport *http.Transport) (client apiClient, err error)
		delay        time.Duration
		failure      error
		lines        []*OslMessage
		tl           lane.TestingLane
		ll           lane.LogLane
		count        atomic.Int32
		opensearchapi.Client
	}
)

func (tc *testClient) Bulk(ctx context.Context, req opensearchapi.BulkReq) (*opensearchapi.BulkResp, error) {
	if tc.delay > 0 {
		time.Sleep(tc.delay)
	}

	if tc.failure != nil {
		return nil, tc.failure
	}

	buf := make([]byte, 64*1024)
	n, err := req.Body.Read(buf)
	if err != nil {
		return nil, err
	}

	newLines := []*OslMessage{}

	lines := strings.Split(string(buf[:n]), "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		var reqJson map[string]any
		if err = json.Unmarshal([]byte(line), &reqJson); err != nil {
			return nil, err
		}

		_, isCreate := reqJson["create"]
		if isCreate {
			continue
		}

		var msg OslMessage
		if err = json.Unmarshal([]byte(line), &msg); err != nil {
			return nil, err
		}

		newLines = append(newLines, &msg)
	}

	tc.count.Add(int32(len(newLines)))
	tc.lines = append(tc.lines, newLines...)

	return &opensearchapi.BulkResp{}, nil
}

func (tc *testClient) install(t *testing.T) {
	tc.orgNewClient = newOpenSearchClient
	newOpenSearchClient = func(openSearchHost string, openSearchPort int, openSearchUser, openSearchPass string, openSearchTransport *http.Transport) (client apiClient, err error) {
		client = tc
		return
	}
	tc.lines = []*OslMessage{}
	t.Cleanup(func() {
		newOpenSearchClient = tc.orgNewClient
	})
}

func (tc *testClient) VerifyEventText(eventText string) bool {

	lineCount := len(strings.Split(eventText, "\n"))
	tc.waitForBulk(lineCount)

	if !tc.tl.VerifyEventText(eventText) {
		fmt.Printf("expected:\n\n%s\n\nactual:\n\n%s\n\n", eventText, tc.tl.EventsToString())
		return false
	}

	return true
}

func (tc *testClient) VerifyReceived(eventText string) bool {
	eventList := []*lane.LaneEvent{}

	if eventText != "" {
		lines := strings.Split(eventText, "\n")
		for _, line := range lines {
			parts := strings.Split(line, "\t")
			if len(parts) != 2 {
				panic("eventText line must have exactly one tab separator")
			}
			eventList = append(eventList, &lane.LaneEvent{Level: parts[0], Message: parts[1]})
		}
	}

	tc.waitForBulk(len(eventList))

	if len(eventList) != len(tc.lines) {
		return false
	}

	for i := 0; i < len(eventList); i++ {
		e1 := eventList[i]
		e2 := tc.lines[i]

		parts := strings.SplitN(e2.LogMessage, " ", 2)
		text := parts[1]
		cutPoint := strings.Index(text, "} ")
		if cutPoint < 0 {
			return false
		}
		text = text[cutPoint+2:]

		if e1.Level != parts[0] || e1.Message != text {
			fmt.Printf("level expected \"%s\", recieved: \"%s\"\n", e1.Level, parts[0])
			fmt.Printf("message expected \"%s\", recieved: \"%s\"\n", e1.Message, text)
			return false
		}
	}

	return true
}

func (tc *testClient) EventsToStringN(eventCount int) string {
	tc.waitForBulk(eventCount)
	return tc.EventsToString()
}

func (tc *testClient) EventsToString() string {
	lines := make([]string, 0, len(tc.lines))
	for _, line := range tc.lines {
		parts := strings.SplitN(line.LogMessage, " ", 2)
		text := parts[1]
		cutPoint := strings.Index(text, "} ")
		if cutPoint < 0 {
			return ""
		}
		text = strings.ReplaceAll(text, "\n", "\\n")
		text = strings.ReplaceAll(text, "\t", "\\t")

		lines = append(lines, parts[0]+" "+text)
	}

	return strings.Join(lines, "\n")
}

func (tc *testClient) waitForBulk(events int) {
	for {
		if tc.count.Load() >= int32(events) {
			return
		}
		time.Sleep(time.Millisecond)
	}
}
