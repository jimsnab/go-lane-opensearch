package osl

import (
	"context"
	"log"
	"net/http"
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
	OslDefaultBackoffLimit = 10 * time.Minute
)

type (

	// Function type for the callback invoked when log messages are about to be lost because OpenSearch cannot be reached.
	OslEmergencyFn func(logBuffer []*OslMessage)

	// Configuration struct for OpenSearch connection settings.
	OslConfig struct {
		offline             bool
		OpenSearchHost      string          `json:"openSearchHost"`
		OpenSearchPort      int             `json:"openSearchPort"`
		OpenSearchUser      string          `json:"openSearchUser"`
		OpenSearchPass      string          `json:"openSearchPass"`
		OpenSearchIndex     string          `json:"openSearchIndex"`
		OpenSearchAppName   string          `json:"openSearchAppName"`
		OpenSearchTransport *http.Transport `json:"openSearchTransport"`
		LogThreshold        int             `json:"logThreshold,omitempty"`
		MaxBufferSize       int             `json:"maxBufferSize,omitempty"`
		BackoffInterval     time.Duration   `json:"backoffInterval,omitempty"`
		BackoffLimit        time.Duration   `json:"BackoffLimit,omitempty"`
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
		lane.LogLane
		mu                   sync.Mutex
		openSearchConnection *openSearchConnection
	}

	// Interface defining methods for a lane in OpenSearch logging.
	OpenSearchLane interface {
		lane.Lane
		Reconnect(config *OslConfig) (err error)
		SetEmergencyHandler(emergencyFn OslEmergencyFn)
		Stats() (stats OslStats)
	}
)

func NewOpenSearchLane(ctx context.Context, config *OslConfig) (l OpenSearchLane, err error) {

	createFn := func(parentLane lane.Lane) (newLane lane.Lane, ll lane.LogLane, writer *log.Logger, err error) {
		newLane, ll, writer, err = createOpenSearchLane(config, parentLane)
		return
	}

	osl, err := lane.NewEmbeddedLogLane(createFn, ctx)
	if err != nil {
		return
	}

	l = osl.(*openSearchLane)
	return
}

func (osl *openSearchLane) Reconnect(config *OslConfig) (err error) {
	return osl.openSearchConnection.connect(config)
}

func createOpenSearchLane(config *OslConfig, parentLane lane.Lane) (newLane lane.Lane, ll lane.LogLane, writer *log.Logger, err error) {
	osl := openSearchLane{}
	posl, _ := parentLane.(*openSearchLane)

	if posl == nil {
		// first instance - instantiate the connection to opensearch
		osl.openSearchConnection, err = newOpenSearchConnection(config)
		if err != nil {
			return
		}
	} else {
		// additional instance - bind to the existing connection
		osl.openSearchConnection = posl.openSearchConnection
	}

	osl.LogLane = lane.AllocEmbeddedLogLane()
	osl.LogLane.SetFlagsMask(log.Ldate | log.Ltime)
	osl.openSearchConnection.attach()
	writer = log.New(&osl, "", 0)

	newLane = &osl
	ll = osl.LogLane
	return
}

func (osl *openSearchLane) Close() {
	osl.openSearchConnection.detach()
}

func (osl *openSearchLane) Write(p []byte) (n int, err error) {
	osl.mu.Lock()
	defer osl.mu.Unlock()

	logEntry := string(p)

	logEntry = strings.TrimRight(logEntry, "\n")

	parentLaneId, _ := osl.LogLane.Value(lane.ParentLaneIdKey).(string)

	lm := osl.LogLane.(lane.LaneMetadata)
	mapCopy := lm.MetadataMap()
	mapCopy["timestamp"] = time.Now().UTC().Format(time.RFC3339)

	logData := OslMessage{
		ParentLaneId: parentLaneId,
		JourneyID:    osl.JourneyId(),
		LaneID:       osl.LaneId(),
		LogMessage:   logEntry,
		Metadata:     mapCopy,
	}

	osl.openSearchConnection.log(logData)

	return len(p), nil
}

func (osl *openSearchLane) Stats() OslStats {
	return osl.openSearchConnection.stats()
}

func (osl *openSearchLane) SetEmergencyHandler(emergencyFn OslEmergencyFn) {
	osl.openSearchConnection.setEmergencyHandler(emergencyFn)
}
