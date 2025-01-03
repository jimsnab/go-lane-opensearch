package osl

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
)

func TestTeeTestDerive(t *testing.T) {
	tc := &testClient{}
	tc.install(t)
	cfg := OslConfig{
		OpenSearchHost:  "localhost",
		OpenSearchPort:  1000,
		OpenSearchIndex: "example",
	}
	osl, err := NewOpenSearchLane(context.Background(), &cfg)
	if err != nil {
		t.Fatal(err)
	}

	tlv := lane.NewTestingLane(context.Background())

	if err != nil {
		t.Fatal(err)
	}
	osl.AddTee(tlv)

	osl.Trace("trace", 1)
	osl.Tracef("%s %d", "tracef", 1)

	tl2 := osl.Derive()
	tl2.Debug("debug", 1)
	tl2.Debugf("%s %d", "debugf", 1)

	tl3, cf := tl2.DeriveWithCancel()
	tl3.Info("info", 1)
	tl3.Infof("%s %d", "infof", 1)
	cf() // free chan resource

	tl4, cf := osl.DeriveWithDeadline(time.Now().Add(time.Hour))
	tl4.Warn("warn", 1)
	tl4.Warnf("%s %d", "warnf", 1)
	cf() // free chan resource

	tl5, cf := tl3.DeriveWithTimeout(time.Hour)
	tl5.Error("error", 1)
	tl5.Errorf("%s %d", "errorf", 1)
	tl5.PreFatal("fatal", 1)
	tl5.PreFatalf("%s %d", "fatalf", 1)
	cf() // free chan resource

	tl6 := tl5.DeriveReplaceContext(context.Background())
	tl6.Trace("trace", 2)

	events := []*lane.LaneEvent{}
	events = append(events, &lane.LaneEvent{Level: "TRACE", Message: "trace 1"})
	events = append(events, &lane.LaneEvent{Level: "TRACE", Message: "tracef 1"})

	events = append(events, &lane.LaneEvent{Level: "DEBUG", Message: "debug 1"})
	events = append(events, &lane.LaneEvent{Level: "DEBUG", Message: "debugf 1"})

	events = append(events, &lane.LaneEvent{Level: "INFO", Message: "info 1"})
	events = append(events, &lane.LaneEvent{Level: "INFO", Message: "infof 1"})

	events = append(events, &lane.LaneEvent{Level: "WARN", Message: "warn 1"})
	events = append(events, &lane.LaneEvent{Level: "WARN", Message: "warnf 1"})

	events = append(events, &lane.LaneEvent{Level: "ERROR", Message: "error 1"})
	events = append(events, &lane.LaneEvent{Level: "ERROR", Message: "errorf 1"})

	events = append(events, &lane.LaneEvent{Level: "FATAL", Message: "fatal 1"})
	events = append(events, &lane.LaneEvent{Level: "FATAL", Message: "fatalf 1"})

	events = append(events, &lane.LaneEvent{Level: "TRACE", Message: "trace 2"})

	if !tlv.VerifyEvents(events) {
		t.Errorf("Test events don't match")
	}
}

func TestTeeTestJourneyId(t *testing.T) {
	serverLane := lane.NewLogLane(nil)

	tc, osl := testMakeFirstOslEx(t, testNoTees)

	serverLane.AddTee(osl)

	requestLane := serverLane.Derive()
	requestLane.SetJourneyId("journey")

	requestLane.Info("test")
	tc.waitForBulk(1)

	sent := tc.EventsToString()
	if !strings.Contains(sent, "INFO {journey:") {
		t.Errorf("journey ID did not pass through")
	}
}
