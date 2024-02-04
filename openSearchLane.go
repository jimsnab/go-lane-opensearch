package osl

import (
	"context"
	"log"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/jimsnab/go-lane"
)

const (
	// Represents the default threshold for logging messages.
	// Determines the size at which the log messages buffer triggers bulk insertion.
	OslDefaultLogThreshold = 100
	// Denotes the default maximum buffer size for log messages.
	// Controls the size limit of the buffer used for storing log messages.
	OslDefaultMaxBufferSize = 100
	// Defines the default interval for the backoff logic.
	// Specifies the duration between consecutive attempts to reconnect or resend messages in case of failures.
	OslDefaultBackoffInterval = 10 * time.Second
	// Specifies the default maximum duration for backoff intervals.
	// Limits the time span within which backoff attempts are made before considering a connection or message sending attempt as failed.
	OslDefaultBackOffLimit = 10 * time.Minute
)

type (

	// Function type for the callback invoked when log messages are about to be lost because OpenSearch cannot be reached.
	OslEmergencyFn func(logBuffer []*OslMessage) (err error)

	// Configuration struct for OpenSearch connection settings.
	OslConfig struct {
		offline             bool
		OpenSearchUrl       string          `json:"openSearchUrl"`
		OpenSearchPort      string          `json:"openSearchPort"`
		OpenSearchUser      string          `json:"openSearchUser"`
		OpenSearchPass      string          `json:"openSearchPass"`
		OpenSearchIndex     string          `json:"openSearchIndex"`
		OpenSearchAppName   string          `json:"openSearchAppName"`
		OpenSearchTransport *http.Transport `json:"openSearchTransport"`
		LogThreshold        int             `json:"logThreshold,omitempty"`
		MaxBufferSize       int             `json:"maxBufferSize,omitempty"`
		BackoffInterval     time.Duration   `json:"backoffInterval,omitempty"`
		BackOffLimit        time.Duration   `json:"backoffLimit,omitempty"`
	}

	// Struct representing a log message in OpenSearch.
	OslMessage struct {
		AppName      string            `json:"appName"`
		ParentLaneId string            `json:"parentLaneId,omitempty"`
		JourneyID    string            `json:"journeyId,omitempty"`
		LaneID       string            `json:"laneId,omitempty"`
		LogMessage   string            `json:"logMessage,omitempty"`
		Metadata     map[string]string `json:"metadata,omitempty"`
	}

	// Struct holding statistics about message queues and sent messages in OpenSearch logging.
	OslStats struct {
		MessagesQueued     int `json:"messagesQueued"`
		MessagesSent       int `json:"messagesSent"`
		MessagesSentFailed int `json:"messagesSentFailed"`
	}

	// Struct representing a lane in OpenSearch logging.
	openSearchLane struct {
		logLane
		openSearchConnection *openSearchConnection
		metadata             map[string]string
	}

	// Interface defining methods for a lane in OpenSearch logging.
	OpenSearchLane interface {
		Lane
		LaneMetadata
		Reconnect(config *OslConfig) (err error)
		SetEmergencyHandler(emergencyFn OslEmergencyFn)
		Stats() (stats OslStats)
	}
)

func NewOpenSearchLane(ctx context.Context, config *OslConfig) (l OpenSearchLane) {
	ll := deriveLogLane(nil, ctx, []Lane{}, "")

	ctx, cancelFn := context.WithCancel(context.Background())

	osl := openSearchLane{
		openSearchConnection: &openSearchConnection{
			logBuffer: make([]*OslMessage, 0),
			stopCh:    make(chan *sync.WaitGroup, 1),
			wakeCh:    make(chan struct{}, 1),
			clientCh:  make(chan *sync.WaitGroup, 1),
			ctx:       ctx,
			cancelFn:  cancelFn,
		},
	}
	osl.openSearchConnection.attach()

	ll.setFlagsMask(log.Ldate | log.Ltime)
	ll.clone(&osl.logLane)
	osl.logLane.writer = log.New(&osl, "", 0)

	val := reflect.ValueOf(config)
	if val.Kind() != reflect.Struct {
		config = &OslConfig{
			offline: true,
		}
		osl.openSearchConnection.config = config
	}

	err := osl.openSearchConnection.connect(config)
	if err != nil {
		osl.Error(err)
	}

	go osl.openSearchConnection.processConnection()

	l = &osl

	return

}

func (osl *openSearchLane) Reconnect(config *OslConfig) (err error) {
	return osl.openSearchConnection.reconnect(config)
}

func (osl *openSearchLane) Derive() Lane {
	ll := deriveLogLane(&osl.logLane, context.WithValue(osl.Context, parent_lane_id, osl.LaneId()), osl.tees, osl.cr)

	osl2 := finishDerive(osl, ll)

	return osl2
}

func (osl *openSearchLane) DeriveWithCancel() (Lane, context.CancelFunc) {
	childCtx, cancelFn := context.WithCancel(context.WithValue(osl.logLane.Context, parent_lane_id, osl.logLane.LaneId()))
	ll := deriveLogLane(&osl.logLane, childCtx, osl.tees, osl.cr)

	osl2 := finishDerive(osl, ll)

	return osl2, cancelFn
}

func (osl *openSearchLane) DeriveReplaceContext(ctx context.Context) Lane {
	ll := deriveLogLane(&osl.logLane, context.WithValue(ctx, parent_lane_id, osl.LaneId()), osl.tees, osl.cr)

	osl2 := finishDerive(osl, ll)

	return osl2
}

func finishDerive(osl *openSearchLane, ll *logLane) *openSearchLane {
	osl2 := openSearchLane{
		openSearchConnection: osl.openSearchConnection,
	}
	osl.openSearchConnection.attach()

	ll.setFlagsMask(log.Ldate | log.Ltime)
	ll.clone(&osl2.logLane)
	osl2.logLane.writer = log.New(&osl2, "", 0)

	return &osl2
}

func (osl *openSearchLane) SetMetadata(key string, val string) {
	osl.mu.Lock()
	defer osl.mu.Unlock()

	if osl.metadata == nil {
		osl.metadata = make(map[string]string)
	}

	osl.metadata[key] = val
}

func (osl *openSearchLane) Close() {
	osl.openSearchConnection.detach()
}

func (osl *openSearchLane) Write(p []byte) (n int, err error) {
	osl.mu.Lock()
	defer osl.mu.Unlock()

	logEntry := string(p)

	logEntry = strings.ReplaceAll(logEntry, "\n", " ")

	parentLaneId, _ := osl.Context.Value(parent_lane_id).(string)

	mapCopy := make(map[string]string, len(osl.metadata)+1)
	if len(osl.metadata) > 0 {
		for k, v := range osl.metadata {
			mapCopy[k] = v
		}
	}
	mapCopy["timestamp"] = time.Now().UTC().Format(time.RFC3339)

	logData := &OslMessage{
		AppName:      osl.openSearchConnection.config.OpenSearchAppName,
		ParentLaneId: parentLaneId,
		JourneyID:    osl.journeyId,
		LaneID:       osl.LaneId(),
		LogMessage:   logEntry,
		Metadata:     mapCopy,
	}

	osl.openSearchConnection.logBuffer = append(osl.openSearchConnection.logBuffer, logData)

	osl.openSearchConnection.messagesQueued++

	if ((1 + osl.openSearchConnection.messagesSent - osl.openSearchConnection.messagesQueued) % osl.openSearchConnection.config.LogThreshold) == 0 {
		osl.openSearchConnection.wakeCh <- struct{}{}
	}

	return len(p), nil
}

func (osl *openSearchLane) Stats() (stats OslStats) {
	osl.mu.Lock()
	defer osl.mu.Unlock()

	stats.MessagesQueued = osl.openSearchConnection.messagesQueued
	stats.MessagesSent = osl.openSearchConnection.messagesSent
	stats.MessagesSentFailed = osl.openSearchConnection.messagesSentFailed

	return
}

func (osl *openSearchLane) SetEmergencyHandler(emergencyFn OslEmergencyFn) {
	osl.mu.Lock()
	defer osl.mu.Unlock()
	osl.openSearchConnection.emergencyFn = emergencyFn
}
