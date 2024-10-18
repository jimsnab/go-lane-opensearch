# go-lane-opensearch

A "lane" is a context that includes logging functionality, combining Go's 'log' 
package with 'context'. Learn more at [go-lane](http://github/jimsnab/go-lane).

This project enables sending lane logs to OpenSearch.

# Usage

## Basic Use
```go
import (
    "context"
    "github.com/jimsnab/go-lane"
	osl "github.com/jimsnab/go-lane-opensearch"
)

func myFunc() {
    l, err := osl.NewOpenSearchLane(nil, &osl.OslConfig{
		OpenSearchHost: "localhost",
		OpenSearchPort: 9200,
		OpenSearchUser: "admin",
		OpenSearchPass: "TheAdmin&1",
		OpenSearchIndex: "logging",
		OpenSearchAppName: "myapp",
		OpenSearchTransport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // insecure: for example only
		},
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()

    l.Info("log something")
}
```

## Tee
It is common to tee the OpenSearchLane with another lane like the standard LogLane,
so that logging goes to OpenSearch, and to stdout.

```go
	l2 := lane.NewLogLane(nil)
	l.AddTee(l2)
```

## Buffering, Retry and Emergency Handler
The OpenSearch lane will collect logging and send documents as a batch. It will retry if
the server is unavailable.

If too many retries occur, the log messages can be sent to an emergency store such as
the local disk, so that diagnostics are not lost in an unstable environment. The client
of the OpenSearch lane implements the emergency storage as needed.

```go
	l.SetEmergencyHandler(myEmergencyHandler)
```

```go
func myEmergencyHandler(logBuffer []*osl.OslMessage) {
	for _, logEntry := range logBuffer {
		logLine := fmt.Sprintf("AppName: %s, ParentLaneId: %s, JourneyID: %s, LaneID: %s, LogMessage: %s, Metadata: %+v\n",
			logEntry.AppName, logEntry.ParentLaneId, logEntry.JourneyID, logEntry.LaneID, logEntry.LogMessage, logEntry.Metadata)

		fmt.Fprintf(os.Stderr, logLine)
	}	
}
```

OpenSearch lane configuration allows the client to specify the size of the buffer for
accumulating logging, and control over the amount of retries.

|OslConfig Member |Description                          |
|-----------------|-------------------------------------|
|`LogThreshold`   | Determines the size at which the log messages buffer triggers bulk insertion. |
|`MaxBufferSize`  | Controls the size limit of the buffer used for storing log messages. |
|`BackoffInterval`|Specifies the duration between consecutive attempts to reconnect or resend messages in case of failures. |
|`BackoffLimit`   | Limits the time span within which backoff attempts are made before considering a connection or message sending attempt as failed. |

## Metadata
The metadata is included with each log message:

* `timestamp` is included and works well with OpenSearch indexing.
* `appName` from the configuration is included with each document.
* `journeyId` stored in the lane is sent, unless it is empty.
* `laneId` provides a unique correlation ID for the lane.
* `parentLaneId` provides the correlation ID of the parent lane, if there is one.
* `logMessage` is the formatted log message.

Additional metadata will be included when added via the standard lane interface for metadata.

## Offline Mode

If the OpenSearch lane is created without a configuration for OpenSearch, it will fall into
offline mode, where it collects log messages, up to the configured buffering limit.

A common pattern is to create an OpenSearch lane right away, before the credentials to
OpenSearch have been obtained. In that way, the process of retrieving the credentials can
be logged. Once the credentials are set in the OpenSearch lane, all of the startup logging
will be uploaded.

## Changing Connection Configuration

The server configuration can be changed at any time.

```go
	err = l.Reconnect(&osl.OslConfig{
		OpenSearchHost: "localhost",
		OpenSearchPort: 9200,
		OpenSearchUser: "admin",
		OpenSearchPass: "TheAdmin&1",
		OpenSearchIndex: "logging",
		OpenSearchAppName: "myapp",
		OpenSearchTransport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // insecure: for example only
		},
	)
	if err != nil {
		l.Fatal(err)
	}
```

To go to offline mode, call `l.Reconnect(nil)`.

## Derivation

When an OpenSearch lane is established, a connection task is created to handle uploads. Derived lanes share this connection task, which is reference-counted to ensure it remains active until all lanes associated with it are closed.

## Closing

While most lane types do not need to be closed, the OpenSearch lane does. Calling `Close()`
ensures all of the uploading is complete prior to termination.
