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
)

// Struct encapsulating the connection and management logic for interacting with OpenSearch.
type (
	openSearchConnection struct {
		mu                 sync.Mutex
		logBuffer          []*OslMessage
		connectCh          chan *connectRequest
		refChangeCh        chan *refRequest
		wakeCh             chan struct{}
		emergencyFn        OslEmergencyFn
		messagesQueued     int
		messagesSent       int
		messagesSentFailed int
		pumpInterval       time.Duration
		cfg                *OslConfig
		flushing           *sync.WaitGroup
		backoffDuration    time.Duration
	}

	connectRequest struct {
		wg     sync.WaitGroup
		config *OslConfig // nil to disconnect
		err    error
	}

	refRequest struct {
		wg     sync.WaitGroup
		change int
	}

	apiClient interface {
		Bulk(ctx context.Context, req opensearchapi.BulkReq) (*opensearchapi.BulkResp, error)
	}
)

var newOpenSearchClient = realNewOpenSearchClient

func newOpenSearchConnection(config *OslConfig) (osc *openSearchConnection, err error) {
	connection := openSearchConnection{
		logBuffer:    []*OslMessage{},
		refChangeCh:  make(chan *refRequest, 1),
		wakeCh:       make(chan struct{}, 1),
		connectCh:    make(chan *connectRequest, 1),
		pumpInterval: time.Second,
	}

	go connection.processConnection()

	if err = connection.connect(config); err != nil {
		return
	}

	osc = &connection
	return
}

func (osc *openSearchConnection) setEmergencyHandler(emergencyFn OslEmergencyFn) {
	osc.mu.Lock()
	defer osc.mu.Unlock()
	osc.emergencyFn = emergencyFn
}

func (osc *openSearchConnection) stats() (stats OslStats) {
	osc.mu.Lock()
	defer osc.mu.Unlock()

	stats.MessagesQueued = osc.messagesQueued
	stats.MessagesSent = osc.messagesSent
	stats.MessagesSentFailed = osc.messagesSentFailed

	return
}

func (osc *openSearchConnection) log(msg OslMessage) {
	var dropped []*OslMessage

	osc.mu.Lock()

	pending := osc.messagesQueued - osc.messagesSent
	if pending >= osc.cfg.MaxBufferSize {
		// have to drop messages
		toRemove := (pending + 1) - osc.cfg.MaxBufferSize
		inFlight := pending - len(osc.logBuffer)
		cutPoint := toRemove + inFlight
		if cutPoint > 0 {
			if cutPoint >= len(osc.logBuffer) {
				cutPoint = len(osc.logBuffer)
			}
			dropped = osc.logBuffer[:cutPoint]
			osc.logBuffer = osc.logBuffer[cutPoint:]
		}
	}

	msg.AppName = osc.cfg.OpenSearchAppName
	osc.logBuffer = append(osc.logBuffer, &msg)
	osc.messagesQueued++

	pending = osc.messagesQueued - osc.messagesSent
	if (pending % osc.cfg.LogThreshold) == 0 {
		osc.mu.Unlock()
		osc.wakeCh <- struct{}{}
	} else {
		osc.mu.Unlock()
	}

	if len(dropped) > 0 {
		osc.mu.Lock()
		osc.messagesSentFailed += len(dropped)
		ef := osc.emergencyFn
		osc.mu.Unlock()

		if ef != nil {
			ef(dropped)
		}
	}
}

func (osc *openSearchConnection) connect(config *OslConfig) (err error) {
	// sanitize the config struct
	cfg := OslConfig{}
	if config == nil {
		cfg.offline = true
	} else {
		cfg = *config
		if cfg.OpenSearchUrl == "" || cfg.OpenSearchPort == 0 || cfg.OpenSearchTransport == nil {
			cfg.offline = true
		}
		if cfg.LogThreshold == 0 {
			cfg.LogThreshold = OslDefaultLogThreshold
		}
		if cfg.MaxBufferSize == 0 {
			cfg.MaxBufferSize = OslDefaultMaxBufferSize
		}
		if cfg.BackoffInterval == 0 {
			cfg.BackoffInterval = OslDefaultBackoffInterval
		}
		if cfg.BackoffLimit == 0 {
			cfg.BackoffLimit = OslDefaultBackoffLimit
		}
	}

	// send it to the processing task
	req := connectRequest{config: &cfg}
	req.wg.Add(1)
	osc.connectCh <- &req
	req.wg.Wait()

	return req.err
}

func (osc *openSearchConnection) processConnection() {
	var client apiClient
	refs := 0

	for {
		osc.mu.Lock()
		pumpInterval := osc.backoffDuration
		osc.mu.Unlock()

		if pumpInterval == 0 {
			pumpInterval = osc.pumpInterval
		}

		select {
		case req := <-osc.connectCh:
			// config change - make a new client
			osc.mu.Lock()
			osc.cfg = req.config
			osc.backoffDuration = 0
			osc.mu.Unlock()

			if req.config.offline {
				client = nil
			} else {
				client, req.err = newOpenSearchClient(
					req.config.OpenSearchUrl,
					req.config.OpenSearchPort,
					req.config.OpenSearchUser,
					req.config.OpenSearchPass,
					req.config.OpenSearchTransport,
				)
			}
			req.wg.Done()

		case req := <-osc.refChangeCh:
			// attach or detatch
			refs += req.change
			if refs <= 0 {
				// last instance disconnected - drain and exit
				osc.flush(client, true)
				req.wg.Done()
				return
			} else {
				req.wg.Done()
			}

		case <-osc.wakeCh:
			// log activity is backing up, drain
			osc.flush(client, false)

		case <-time.After(pumpInterval):
			// regular wait time interval has expired - drain
			osc.flush(client, false)
		}
	}
}

func (osc *openSearchConnection) flush(client apiClient, final bool) {
	// if not connected, don't do anything - unless this is the final call, for which
	// anything unsent must be passed to the emergency write fn
	if client == nil && !final {
		return
	}

	osc.mu.Lock()
	// if previously flushing, and not final, let that finish
	if osc.flushing != nil {
		pwg := osc.flushing
		osc.mu.Unlock()
		if !final {
			return
		}

		(*pwg).Wait()

		osc.mu.Lock()
		osc.flushing = nil
	}

	// take ownership of the log buffer (unless it is empty)
	if len(osc.logBuffer) == 0 {
		osc.mu.Unlock()
		osc.backoffDuration = 0
		return
	}

	logBuffer := osc.logBuffer
	osc.logBuffer = make([]*OslMessage, 0, len(logBuffer))
	osc.mu.Unlock()

	if client == nil {
		// final - not connected - save to emergency log
		if osc.emergencyFn != nil {
			osc.emergencyFn(osc.logBuffer)
		}
		return
	}

	// send to opensearch
	var wg sync.WaitGroup
	osc.flushing = &wg
	wg.Add(1)
	go func() {
		osc.mu.Lock()
		backoffDuration := osc.backoffDuration
		osc.mu.Unlock()

		defer func() {
			osc.mu.Lock()
			osc.backoffDuration = backoffDuration
			osc.flushing = nil
			osc.mu.Unlock()
			wg.Done()
		}()

		err := osc.bulkInsert(client, logBuffer)
		// upon a failure, try again after a backoff; and give up if it takes too long
		if err != nil {

			if backoffDuration == 0 {
				backoffDuration = osc.cfg.BackoffInterval
			} else {
				backoffDuration *= 2
			}

			if (backoffDuration > osc.cfg.BackoffLimit) || final {
				// waited too long or is final - losing this set of messages - send to emergency log
				backoffDuration = osc.cfg.BackoffInterval
				if osc.emergencyFn != nil {
					osc.emergencyFn(logBuffer)
				}
				err = nil
			}

			osc.mu.Lock()
			if err != nil {
				// failed to send - put the messages back in the queue and retry
				osc.logBuffer = append(logBuffer, osc.logBuffer...)
			} else {
				// dropped the messages
				osc.messagesSentFailed += len(logBuffer)
			}
			osc.mu.Unlock()
		} else {
			osc.mu.Lock()
			osc.messagesSent += len(logBuffer)
			osc.mu.Unlock()
			backoffDuration = 0
		}
	}()
	wg.Wait()
}

func (osc *openSearchConnection) attach() {
	req := refRequest{
		change: 1,
	}
	req.wg.Add(1)
	osc.refChangeCh <- &req
	req.wg.Wait()
}

func (osc *openSearchConnection) detach() {
	req := refRequest{
		change: -1,
	}
	req.wg.Add(1)
	osc.refChangeCh <- &req
	req.wg.Wait()
}

func (osc *openSearchConnection) bulkInsert(client apiClient, logBuffer []*OslMessage) (err error) {

	jsonData, err := osc.generateBulkJson(logBuffer)
	if err != nil {
		return
	}

	_, err = client.Bulk(context.Background(), opensearchapi.BulkReq{Body: strings.NewReader(jsonData)})
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
		createAction := map[string]any{"create": map[string]any{"_index": osc.cfg.OpenSearchIndex}}
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
	if osc.emergencyFn != nil {
		msg := fmt.Sprintf(formatStr, args...)

		oslm := &OslMessage{
			AppName:    "OpenSearchLane",
			LogMessage: msg,
		}

		oslm.Metadata = make(map[string]string)
		oslm.Metadata["timestamp"] = time.Now().UTC().Format(time.RFC3339)

		logBuffer := []*OslMessage{oslm}

		osc.emergencyFn(logBuffer)
	}
}

func realNewOpenSearchClient(openSearchUrl string, openSearchPort int, openSearchUser, openSearchPass string, openSearchTransport *http.Transport) (client apiClient, err error) {
	apicli, err := opensearchapi.NewClient(
		opensearchapi.Config{
			Client: opensearch.Config{
				Transport: openSearchTransport,
				Addresses: []string{fmt.Sprintf("%s:%d", openSearchUrl, openSearchPort)},
				Username:  openSearchUser,
				Password:  openSearchPass,
			},
		},
	)
	if err != nil {
		return
	}
	client = apicli
	return
}
