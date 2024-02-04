package osl

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/opensearch-project/opensearch-go/v3"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
	"github.com/jimsnab/go-lane"
)

// Struct encapsulating the connection and management logic for interacting with OpenSearch.
type openSearchConnection struct {
	client             *opensearchapi.Client
	clientMu           sync.Mutex
	mu                 sync.Mutex
	logBuffer          []*OslMessage
	stopCh             chan *sync.WaitGroup
	wakeCh             chan struct{}
	clientCh           chan *sync.WaitGroup
	refCount           int
	config             *OslConfig
	emergencyFn        OslEmergencyFn
	ctx                context.Context
	cancelFn           context.CancelFunc
	messagesQueued     int
	messagesSent       int
	messagesSentFailed int
}

func (osc *openSearchConnection) connect(config *OslConfig) (err error) {

	var client *opensearchapi.Client

	if config == nil || !config.offline {
		var wg sync.WaitGroup
		wg.Add(1)
		osc.clientCh <- &wg
		wg.Wait()
	}

	osc.clientMu.Lock()

	if config == nil {
		config = &OslConfig{offline: true}
	}

	if !config.offline {
		client, err = newOpenSearchClient(config.OpenSearchUrl, config.OpenSearchPort, config.OpenSearchUser, config.OpenSearchPass, config.OpenSearchTransport)
		if err != nil {
			return
		}
	}

	osc.client = client
	osc.config = config

	if config.LogThreshold == 0 {
		osc.config.LogThreshold = OslDefaultLogThreshold
	}
	if config.MaxBufferSize == 0 {
		osc.config.MaxBufferSize = OslDefaultMaxBufferSize
	}
	if config.BackoffInterval == 0 {
		osc.config.BackoffInterval = OslDefaultBackoffInterval
	}
	if config.BackOffLimit == 0 {
		osc.config.BackOffLimit = OslDefaultBackOffLimit
	}
	osc.clientMu.Unlock()

	return

}

func (osc *openSearchConnection) reconnect(config *OslConfig) (err error) {
	return osc.connect(config)
}

func (osc *openSearchConnection) processConnection() {
	backoffDuration := osc.config.BackoffInterval

	for {
		select {
		case wg := <-osc.stopCh:
			shouldStop := false
			osc.mu.Lock()
			osc.refCount--
			shouldStop = (osc.refCount == 0)
			osc.mu.Unlock()

			if shouldStop {
				go func() {
					osc.mu.Lock()
					if len(osc.logBuffer) > 0 {
						err := osc.flush(osc.logBuffer)
						if err != nil {
							if osc.emergencyFn != nil {
								err = osc.emergencyFn(osc.logBuffer)
								if err != nil {
									osc.messagesSentFailed += len(osc.logBuffer)
								} else {
									osc.messagesSent += len(osc.logBuffer)
								}
							}
						}
						osc.logBuffer = make([]*OslMessage, 0)
					}
					osc.mu.Unlock()
					wg.Done()
				}()
				return
			}

			wg.Done()

		case <-osc.wakeCh:
			if !osc.config.offline {
				backoffDuration = osc.send(backoffDuration)
			}
			if osc.config.offline && osc.emergencyFn != nil {
				osc.handleOfflineWriteToEmergencyFn()
			}
		case <-time.After(backoffDuration):
			if !osc.config.offline {
				backoffDuration = osc.send(backoffDuration)
			}
			if osc.config.offline && osc.emergencyFn != nil {
				osc.handleOfflineWriteToEmergencyFn()
			}
		case wg := <-osc.clientCh:
			if osc.client != nil && osc.client.Client != nil {
				backoffDuration = osc.send(backoffDuration)
				osc.client = nil
			}
			wg.Done()
		}

	}

}

func (osc *openSearchConnection) send(backoffDuration time.Duration) time.Duration {
	osc.mu.Lock()
	logBuffer := osc.logBuffer
	osc.logBuffer = make([]*OslMessage, 0)
	osc.mu.Unlock()

	if len(logBuffer) > 0 {
		osc.clientMu.Lock()
		err := osc.flush(logBuffer)
		if err != nil {
			if len(logBuffer) > osc.config.MaxBufferSize {
				err = osc.emergencyFn(logBuffer)
				if err != nil {
					osc.messagesSentFailed += len(logBuffer)
				} else {
					osc.messagesSent += len(logBuffer)
				}
				backoffDuration = osc.config.BackoffInterval
			} else {
				backoffDuration *= 2
				if backoffDuration > osc.config.BackOffLimit {
					backoffDuration = osc.config.BackOffLimit
				}
				osc.mu.Lock()
				osc.logBuffer = append(logBuffer, osc.logBuffer...)
				osc.mu.Unlock()
			}
		} else {
			osc.messagesSent += len(logBuffer)
			backoffDuration = osc.config.BackoffInterval
			osc.messagesQueued = 0
		}
		osc.clientMu.Unlock()
	}

	return backoffDuration
}

func (osc *openSearchConnection) attach() {
	osc.mu.Lock()
	defer osc.mu.Unlock()
	osc.refCount++
}

func (osc *openSearchConnection) detach() {
	var wg sync.WaitGroup
	wg.Add(1)
	osc.stopCh <- &wg
	wg.Wait()
}

func (osc *openSearchConnection) flush(logBuffer []*OslMessage) (err error) {
	if len(logBuffer) > 0 {
		err = osc.bulkInsert(logBuffer)
		if err != nil {
			return
		}
	}
	return
}

func (osc *openSearchConnection) bulkInsert(logBuffer []*OslMessage) (err error) {

	jsonData, err := osc.generateBulkJson(logBuffer)
	if err != nil {
		return
	}

	_, err = osc.client.Bulk(context.Background(), opensearchapi.BulkReq{Body: strings.NewReader(jsonData)})
	if err != nil {
		osc.emergencyLog("Error while storing values in opensearch: %v", err)
		return
	}

	return
}

func (osc *openSearchConnection) generateBulkJson(logBuffer []*OslMessage) (jsonData string, err error) {
	var lines []string
	var createLine []byte
	var logDataLine []byte

	for _, logData := range logBuffer {
		createAction := map[string]interface{}{"create": map[string]interface{}{"_index": osc.config.OpenSearchIndex}}
		createLine, err = json.Marshal(createAction)
		if err != nil {
			osc.emergencyLog("Error marshalling createAction JSON: %v", err)
			return
		}

		logDataLine, err = json.Marshal(logData)
		if err != nil {
			osc.emergencyLog("Error marshalling logData JSON: %v", err)
			return
		}

		lines = append(lines, string(createLine), string(logDataLine))
	}

	jsonData = strings.Join(lines, "\n") + "\n"

	return
}

func (osc *openSearchConnection) emergencyLog(formatStr string, args ...any) {
	msg := fmt.Sprintf(formatStr, args...)

	oslm := &OslMessage{
		AppName:    "OpenSearchLane",
		LogMessage: msg,
	}

	oslm.Metadata = make(map[string]string)
	oslm.Metadata["timestamp"] = time.Now().UTC().Format(time.RFC3339)

	logBuffer := []*OslMessage{oslm}

	if osc.emergencyFn != nil {
		err := osc.emergencyFn(logBuffer)
		if err != nil {
			osc.messagesSentFailed += len(logBuffer)
		} else {
			osc.messagesSent += len(logBuffer)
		}
	}
}

func newOpenSearchClient(openSearchUrl, openSearchPort, openSearchUser, openSearchPass string, openSearchTransport *http.Transport) (client *opensearchapi.Client, err error) {
	client, err = opensearchapi.NewClient(
		opensearchapi.Config{
			Client: opensearch.Config{
				Transport: openSearchTransport,
				Addresses: []string{fmt.Sprintf("%s:%s", openSearchUrl, openSearchPort)},
				Username:  openSearchUser,
				Password:  openSearchPass,
			},
		},
	)
	return
}

func (osc *openSearchConnection) handleOfflineWriteToEmergencyFn() {
	osc.mu.Lock()
	defer osc.mu.Unlock()
	if len(osc.logBuffer) > 0 {
		err := osc.emergencyFn(osc.logBuffer)
		if err != nil {
			osc.messagesSentFailed += len(osc.logBuffer)
		} else {
			osc.messagesSent += len(osc.logBuffer)
		}
		osc.logBuffer = make([]*OslMessage, 0)
	}
}
