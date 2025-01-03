package osl

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jimsnab/go-lane"
)

type testKeyType string
type testValueType string
type testInitFlag int

const kTestStr testKeyType = "test"
const kTestBase testKeyType = "base"
const kTestReplaced testValueType = "replaced"

const (
	testNoTees testInitFlag = 1 << iota
	testMax10
	testOffline
	testSlow
	testBulkError
	testNoIndex
)

func testMakeFirstOsl(t *testing.T) (tc *testClient, osl OpenSearchLane) {
	return testMakeFirstOslEx(t, 0)
}

func testMakeFirstOslEx(t *testing.T, flags testInitFlag) (tc *testClient, osl OpenSearchLane) {
	tc = &testClient{}
	tc.install(t)
	cfg := OslConfig{}

	if (flags & testNoIndex) == 0 {
		cfg.OpenSearchIndex = "testing"
	}

	var pcfg *OslConfig
	if (flags & testMax10) != 0 {
		cfg.LogThreshold = 10
		cfg.MaxBufferSize = 10
		cfg.BackoffInterval = time.Millisecond
		cfg.BackoffLimit = time.Millisecond * 10
		pcfg = &cfg
	}

	if (flags & testBulkError) != 0 {
		tc.failure = os.ErrPermission
		cfg.LogThreshold = 10
		cfg.BackoffInterval = time.Millisecond
		cfg.BackoffLimit = time.Millisecond * 10
	}

	osl, err := NewOpenSearchLane(context.Background(), pcfg)
	if err != nil {
		t.Fatal(err)
	}

	if (flags & testSlow) != 0 {
		cfg.LogThreshold = 1
		tc.delay = time.Millisecond * 250
	}

	if (flags & testNoTees) == 0 {
		tc.tl = lane.NewTestingLane(context.Background())
		osl.AddTee(tc.tl)

		l := lane.NewLogLane(context.Background())
		tc.ll = l.(lane.LogLane)
		osl.AddTee(tc.ll)
	}

	p := osl.(*openSearchLane)
	p.openSearchConnection.pumpInterval = time.Millisecond * 25

	if (flags & testOffline) == 0 {
		cfg.OpenSearchHost = "localhost"
		cfg.OpenSearchPort = 1000
		cfg.OpenSearchTransport = &http.Transport{}

		if err = osl.Reconnect(&cfg); err != nil {
			t.Fatal(err)
		}
	}
	return
}

func TestOsl(t *testing.T) {
	tc, osl := testMakeFirstOsl(t)

	lid := osl.LaneId()
	if len(lid) != 10 {
		t.Errorf("wrong lane id length %d", len(lid))
	}

	ctx := context.WithValue(tc.tl, kTestStr, "pass")

	events := []*lane.LaneEvent{}
	events2 := []*lane.LaneEvent{}
	osl.Trace("test", "of", "trace")
	events = append(events, &lane.LaneEvent{Level: "TRACE", Message: "test of trace"})
	osl.Tracef("testing %d", 123)
	events = append(events, &lane.LaneEvent{Level: "TRACE", Message: "testing 123"})

	osl.Debug("test", "of", "debug")
	events = append(events, &lane.LaneEvent{Level: "DEBUG", Message: "test of debug"})
	events2 = append(events2, &lane.LaneEvent{Level: "DEBUG", Message: "test of debug"})
	osl.Debugf("testing %d", 456)
	events = append(events, &lane.LaneEvent{Level: "DEBUG", Message: "testing 456"})

	osl.Info("test", "of", "info")
	events = append(events, &lane.LaneEvent{Level: "INFO", Message: "test of info"})
	osl.Infof("testing %d", 789)
	events = append(events, &lane.LaneEvent{Level: "INFO", Message: "testing 789"})
	events2 = append(events2, &lane.LaneEvent{Level: "INFO", Message: "testing 789"})

	osl.Warn("test", "of", "warn")
	events = append(events, &lane.LaneEvent{Level: "WARN", Message: "test of warn"})
	osl.Warnf("testing %d", 1011)
	events = append(events, &lane.LaneEvent{Level: "WARN", Message: "testing 1011"})

	osl.Error("test", "of", "error")
	events = append(events, &lane.LaneEvent{Level: "ERROR", Message: "test of error"})
	osl.Errorf("testing %d", 1213)
	events = append(events, &lane.LaneEvent{Level: "ERROR", Message: "testing 1213"})
	events2 = append(events2, &lane.LaneEvent{Level: "ERROR", Message: "testing 1213"})

	if !tc.tl.VerifyEvents(events) || tc.tl.VerifyEvents(events2) {
		t.Errorf("Test events don't match")
	}

	if !tc.tl.FindEvents(events) || !tc.tl.FindEvents(events2) {
		t.Errorf("Test events don't match 2")
	}

	if ctx.Value(kTestStr) != string("pass") {
		t.Errorf("Context is not working")
	}
}

func TestOslSetLevel(t *testing.T) {
	_, osl := testMakeFirstOsl(t)

	level := osl.SetLogLevel(lane.LogLevelFatal)
	if level != lane.LogLevelTrace {
		t.Error("Log level not initially trace")
	}

	level = osl.SetLogLevel(lane.LogLevelDebug)
	if level != lane.LogLevelFatal {
		t.Error("Log level was not fatal")
	}

	level = osl.SetLogLevel(lane.LogLevelDebug)
	if level != lane.LogLevelDebug {
		t.Error("Log level was not debug")
	}
}

func TestOslInheritLevel(t *testing.T) {
	_, osl := testMakeFirstOsl(t)

	level := osl.SetLogLevel(lane.LogLevelFatal)
	if level != lane.LogLevelTrace {
		t.Error("Log level not initially trace")
	}

	tl2 := osl.Derive()

	level = tl2.SetLogLevel(lane.LogLevelDebug)
	if level != lane.LogLevelFatal {
		t.Error("Log level 2 was not fatal")
	}
}

func TestOslWithCancel(t *testing.T) {
	_, osl := testMakeFirstOsl(t)

	level := osl.SetLogLevel(lane.LogLevelFatal)
	if level != lane.LogLevelTrace {
		t.Error("Log level not initially trace")
	}

	tl2, cancel := osl.DeriveWithCancel()

	isDone := make(chan struct{})

	go func() {
		<-tl2.Done()
		isDone <- struct{}{}
	}()

	level = tl2.SetLogLevel(lane.LogLevelDebug)
	if level != lane.LogLevelFatal {
		t.Error("Log level 2 was not fatal")
	}

	time.Sleep(time.Millisecond)
	cancel()

	<-isDone
}

func TestOslWithTimeoutCancel(t *testing.T) {
	_, osl := testMakeFirstOsl(t)

	level := osl.SetLogLevel(lane.LogLevelFatal)
	if level != lane.LogLevelTrace {
		t.Error("Log level not initially trace")
	}

	tl2, cancel := osl.DeriveWithTimeout(time.Hour)

	isDone := make(chan struct{})

	start := time.Now()
	go func() {
		<-tl2.Done()
		isDone <- struct{}{}
	}()

	level = tl2.SetLogLevel(lane.LogLevelDebug)
	if level != lane.LogLevelFatal {
		t.Error("Log level 2 was not fatal")
	}

	time.Sleep(time.Millisecond)
	cancel()

	<-isDone

	delta := time.Since(start)
	if delta.Milliseconds() > 60 {
		t.Error("Timeout too long")
	}
}

func TestOslWithTimeoutExpire(t *testing.T) {
	_, osl := testMakeFirstOsl(t)

	level := osl.SetLogLevel(lane.LogLevelFatal)
	if level != lane.LogLevelTrace {
		t.Error("Log level not initially trace")
	}

	tl2, _ := osl.DeriveWithTimeout(time.Millisecond)

	isDone := make(chan struct{})

	start := time.Now()
	go func() {
		<-tl2.Done()
		isDone <- struct{}{}
	}()

	level = tl2.SetLogLevel(lane.LogLevelDebug)
	if level != lane.LogLevelFatal {
		t.Error("Log level 2 was not fatal")
	}

	<-isDone

	delta := time.Since(start)
	if delta.Milliseconds() > 60 {
		t.Error("Timeout too long")
	}
}

func TestOslWithDeadlineCancel(t *testing.T) {
	_, osl := testMakeFirstOsl(t)

	level := osl.SetLogLevel(lane.LogLevelFatal)
	if level != lane.LogLevelTrace {
		t.Error("Log level not initially trace")
	}

	start := time.Now()
	tl2, cancel := osl.DeriveWithDeadline(start.Add(time.Minute))

	isDone := make(chan struct{})

	go func() {
		<-tl2.Done()
		isDone <- struct{}{}
	}()

	level = tl2.SetLogLevel(lane.LogLevelDebug)
	if level != lane.LogLevelFatal {
		t.Error("Log level 2 was not fatal")
	}

	time.Sleep(time.Millisecond)
	cancel()

	<-isDone

	delta := time.Since(start)
	if delta.Milliseconds() > 60 {
		t.Error("Timeout too long")
	}
}

func TestOslWithDeadlineExpire(t *testing.T) {
	_, osl := testMakeFirstOsl(t)

	level := osl.SetLogLevel(lane.LogLevelFatal)
	if level != lane.LogLevelTrace {
		t.Error("Log level not initially trace")
	}

	start := time.Now()
	tl2, _ := osl.DeriveWithDeadline(start.Add(time.Millisecond * 10))

	isDone := make(chan struct{})

	go func() {
		<-tl2.Done()
		isDone <- struct{}{}
	}()

	level = tl2.SetLogLevel(lane.LogLevelDebug)
	if level != lane.LogLevelFatal {
		t.Error("Log level 2 was not fatal")
	}

	<-isDone

	delta := time.Since(start)
	if delta.Milliseconds() > 60 {
		t.Error("Timeout too long")
	}
}

func TestOslReplaceContext(t *testing.T) {
	c1 := context.WithValue(context.Background(), kTestBase, kTestBase)
	cfg := OslConfig{
		OpenSearchHost:  "localhost",
		OpenSearchPort:  1000,
		OpenSearchIndex: "sample",
	}
	osl, err := NewOpenSearchLane(c1, &cfg)
	if err != nil {
		t.Fatal(err)
	}

	c2 := context.WithValue(context.Background(), kTestBase, kTestReplaced)
	tl2 := osl.DeriveReplaceContext(c2)

	if tl2.Value(kTestBase) != kTestReplaced {
		t.Error("Base not replaced")
	}

	p := tl2.(*openSearchLane)
	if p.openSearchConnection.cfg.OpenSearchIndex != "sample" {
		t.Fatal("not inherited")
	}

	tl3 := tl2.Derive()
	if tl3.Value(kTestBase) != kTestReplaced {
		t.Error("Derived incorrect")
	}
}

func TestOslVerifyText(t *testing.T) {
	tc, osl := testMakeFirstOsl(t)

	osl.Trace("test", "of", "trace")
	osl.Tracef("testing %d", 123)

	osl.Debug("test", "of", "debug")
	osl.Debugf("testing %d", 456)

	osl.Info("test", "of", "info")
	osl.Infof("testing %d", 789)

	osl.Warn("test", "of", "warn")
	osl.Warnf("testing %d", 1011)

	osl.Error("test", "of", "error")
	osl.Errorf("testing %d", 1213)

	expected := `TRACE	test of trace
TRACE	testing 123
DEBUG	test of debug
DEBUG	testing 456
INFO	test of info
INFO	testing 789
WARN	test of warn
WARN	testing 1011
ERROR	test of error
ERROR	testing 1213`

	if !tc.VerifyReceived(expected) {
		t.Errorf("Test events don't match")
	}
}

func TestOslVerifyTextTrace(t *testing.T) {
	tc, osl := testMakeFirstOsl(t)

	osl.SetLogLevel(lane.LogLevelDebug)

	osl.Trace("test", "of", "trace")
	osl.Tracef("testing %d", 123)

	osl.Debug("test", "of", "debug")
	osl.Debugf("testing %d", 456)

	osl.Info("test", "of", "info")
	osl.Infof("testing %d", 789)

	osl.Warn("test", "of", "warn")
	osl.Warnf("testing %d", 1011)

	osl.Error("test", "of", "error")
	osl.Errorf("testing %d", 1213)

	expected := `DEBUG	test of debug
DEBUG	testing 456
INFO	test of info
INFO	testing 789
WARN	test of warn
WARN	testing 1011
ERROR	test of error
ERROR	testing 1213`

	if !tc.VerifyReceived(expected) {
		t.Errorf("Test events don't match")
	}
}

func TestOslVerifyTextDebug(t *testing.T) {
	tc, osl := testMakeFirstOsl(t)

	osl.SetLogLevel(lane.LogLevelInfo)

	osl.Trace("test", "of", "trace")
	osl.Tracef("testing %d", 123)

	osl.Debug("test", "of", "debug")
	osl.Debugf("testing %d", 456)

	osl.Info("test", "of", "info")
	osl.Infof("testing %d", 789)

	osl.Warn("test", "of", "warn")
	osl.Warnf("testing %d", 1011)

	osl.Error("test", "of", "error")
	osl.Errorf("testing %d", 1213)

	expected := `INFO	test of info
INFO	testing 789
WARN	test of warn
WARN	testing 1011
ERROR	test of error
ERROR	testing 1213`

	if !tc.VerifyReceived(expected) {
		t.Errorf("Test events don't match")
	}
}

func TestOslVerifyTextInfo(t *testing.T) {
	tc, osl := testMakeFirstOsl(t)

	osl.SetLogLevel(lane.LogLevelWarn)

	osl.Trace("test", "of", "trace")
	osl.Tracef("testing %d", 123)

	osl.Debug("test", "of", "debug")
	osl.Debugf("testing %d", 456)

	osl.Info("test", "of", "info")
	osl.Infof("testing %d", 789)

	osl.Warn("test", "of", "warn")
	osl.Warnf("testing %d", 1011)

	osl.Error("test", "of", "error")
	osl.Errorf("testing %d", 1213)

	expected := `WARN	test of warn
WARN	testing 1011
ERROR	test of error
ERROR	testing 1213`

	if !tc.VerifyReceived(expected) {
		t.Errorf("Test events don't match")
	}
}

func TestOslVerifyTextWarn(t *testing.T) {
	tc, osl := testMakeFirstOsl(t)

	osl.SetLogLevel(lane.LogLevelError)

	osl.Trace("test", "of", "trace")
	osl.Tracef("testing %d", 123)

	osl.Debug("test", "of", "debug")
	osl.Debugf("testing %d", 456)

	osl.Info("test", "of", "info")
	osl.Infof("testing %d", 789)

	osl.Warn("test", "of", "warn")
	osl.Warnf("testing %d", 1011)

	osl.Error("test", "of", "error")
	osl.Errorf("testing %d", 1213)

	expected := `ERROR	test of error
ERROR	testing 1213`

	if !tc.VerifyReceived(expected) {
		t.Errorf("Test events don't match")
	}
}

func TestOslVerifyTextError(t *testing.T) {
	tc, osl := testMakeFirstOsl(t)

	osl.SetLogLevel(lane.LogLevelFatal)

	osl.Trace("test", "of", "trace")
	osl.Tracef("testing %d", 123)

	osl.Debug("test", "of", "debug")
	osl.Debugf("testing %d", 456)

	osl.Info("test", "of", "info")
	osl.Infof("testing %d", 789)

	osl.Warn("test", "of", "warn")
	osl.Warnf("testing %d", 1011)

	osl.Error("test", "of", "error")
	osl.Errorf("testing %d", 1213)

	osl.SetLogLevel(lane.LogLevelInfo)
	osl.Info("test", "of", "info")

	expected := "INFO\ttest of info"

	if !tc.VerifyReceived(expected) {
		t.Errorf("Test events don't match")
	}
}

func TestOslVerifyCancel(t *testing.T) {
	tc, osl := testMakeFirstOsl(t)
	l, cancelFn := osl.DeriveWithCancel()

	l.Trace("test of trace")

	expected := "TRACE\ttest of trace"

	if !tc.VerifyReceived(expected) {
		t.Errorf("Test events don't match")
	}

	select {
	case <-osl.Done():
		t.Errorf("not yet canceled")
	case <-l.Done():
		t.Errorf("not yet canceled")
	default:
		break
	}

	cancelFn()

	select {
	case <-osl.Done():
		t.Errorf("parent should not be canceled")
	default:
		break
	}

	select {
	case <-l.Done():
		break
	default:
		t.Errorf("child should not be canceled")
	}

	if osl.LaneId() == l.LaneId() {
		t.Errorf("Lane IDs match")
	}

	if len(l.LaneId()) < 6 {
		t.Errorf("insufficient lane id")
	}
}

func TestOslVerifyTimeout(t *testing.T) {
	tc, osl := testMakeFirstOsl(t)

	l, cancelFn := osl.DeriveWithTimeout(time.Hour)

	l.Trace("test of trace")

	expected := "TRACE\ttest of trace"

	if !tc.VerifyReceived(expected) {
		t.Errorf("Test events don't match")
	}

	select {
	case <-l.Done():
		t.Errorf("not yet canceled")
	default:
		break
	}

	cancelFn()

	select {
	case <-l.Done():
		break
	default:
		t.Errorf("should be canceled")
	}

	if osl.LaneId() == l.LaneId() {
		t.Errorf("Lane IDs match")
	}

	if len(l.LaneId()) < 6 {
		t.Errorf("insufficient lane id")
	}
}

func TestOslVerifyDeadline(t *testing.T) {
	tc, osl := testMakeFirstOsl(t)

	l, cancelFn := osl.DeriveWithDeadline(time.Now().Add(time.Hour))

	l.Trace("test of trace")

	expected := "TRACE\ttest of trace"

	if !tc.VerifyReceived(expected) {
		t.Errorf("Test events don't match")
	}

	select {
	case <-l.Done():
		t.Errorf("not yet canceled")
	default:
		break
	}

	cancelFn()

	select {
	case <-l.Done():
		break
	default:
		t.Errorf("should be canceled")
	}

	if osl.LaneId() == l.LaneId() {
		t.Errorf("Lane IDs match")
	}

	if len(l.LaneId()) < 6 {
		t.Errorf("insufficient lane id")
	}
}

func TestOslWrappedLogger(t *testing.T) {
	tc, osl := testMakeFirstOsl(t)

	osl.Logger().Println("this is a test")

	if !tc.VerifyReceived("INFO\tthis is a test") {
		t.Errorf("Test events don't match")
	}
}

func TestOslDerived(t *testing.T) {
	_, posl := testMakeFirstOslEx(t, testNoTees)
	osl := posl.Derive().(OpenSearchLane)

	ptl := lane.NewTestingLane(context.Background())
	posl.AddTee(ptl)
	tl := lane.NewTestingLane(context.Background())
	osl.AddTee(tl)

	posl.Logger().Println("this is the parent")
	osl.Logger().Println("this is the child")

	if !ptl.VerifyEventText("INFO\tthis is the parent\\n") {
		t.Errorf("Test events don't match")
	}

	if !tl.VerifyEventText("INFO\tthis is the child\\n") {
		t.Errorf("Test events don't match")
	}
}

func TestOslJourneyId(t *testing.T) {
	tc, osl := testMakeFirstOslEx(t, testNoTees)

	id := uuid.New().String()
	id = id[len(id)-10:]
	osl.SetJourneyId(id)

	osl.Info("test", "of", "info")

	capture := tc.EventsToStringN(1)
	if !strings.Contains(capture, id) {
		t.Error("did not find outer correlation id")
	}
	if !strings.Contains(capture, osl.LaneId()) {
		t.Error("did not find lane correlation id")
	}
}

func TestOslJourneyIdDerived(t *testing.T) {
	tc, osl := testMakeFirstOslEx(t, testNoTees)

	id := uuid.New().String()
	id = id[len(id)-10:]
	osl.SetJourneyId(id)

	osl2 := osl.Derive()

	osl2.Info("test", "of", "info")

	capture := tc.EventsToStringN(1)
	if !strings.Contains(capture, id) {
		t.Error("did not find outer correlation id")
	}
	if strings.Contains(capture, osl.LaneId()) {
		t.Error("found unexpected correlation id")
	}
	if !strings.Contains(capture, osl2.LaneId()) {
		t.Error("did not find lane correlation id")
	}
}

func verifyLaneEvents(t *testing.T, ll lane.Lane, expected string, output string) {
	v := ll.Value(lane.LogLaneIdKey)
	if v == nil {
		t.Fatal("missing lane id in context")
	}

	guid := v.(string)
	expected = strings.ReplaceAll(expected, "GUID", guid)

	if expected == "" {
		if output != "" {
			t.Fatal("did not get expected empty log")
		}
	} else {
		expectedLines := strings.Split(expected, "\n")
		actualLines := strings.Split(strings.TrimSpace(output), "\n")

		if len(expectedLines) != len(actualLines) {
			t.Fatal("did not get expected number of log lines")
		}

		for i, actualLine := range actualLines {
			expectedLine := expectedLines[i]
			if actualLine != expectedLine {
				if !strings.HasSuffix(expectedLine, "{ANY}") || !strings.HasPrefix(actualLine, expectedLine[:len(expectedLine)-5]) {
					t.Errorf("log events don't match:\n '%s' vs expected\n '%s'", actualLine, expectedLine)
				}
			}
		}
	}
}

func TestOslEnableStack(t *testing.T) {
	_, osl := testMakeFirstOslEx(t, testNoTees)

	for level := lane.LogLevelTrace; level <= lane.LogLevelFatal; level++ {
		v := osl.EnableStackTrace(level, true)
		if v {
			t.Error("expected false")
		}

		v = osl.EnableStackTrace(level, true)
		if !v {
			t.Error("expected true")
		}
	}

	for level := lane.LogLevelTrace; level <= lane.LogLevelFatal; level++ {
		v := osl.EnableStackTrace(level, false)
		if !v {
			t.Error("expected false")
		}

		v = osl.EnableStackTrace(level, false)
		if v {
			t.Error("expected false")
		}
	}
}

func TestOslEnableStack2(t *testing.T) {
	tc, osl := testMakeFirstOslEx(t, testNoTees)

	v := osl.EnableStackTrace(lane.LogLevelError, true)
	if v {
		t.Error("expected false")
	}

	osl.Error("test", "of", "error")
	osl.Errorf("testing %d", 1213)

	tc.waitForBulk(14)

	expected := `ERROR {GUID} test of error
STACK {GUID} {ANY}
STACK {GUID} {ANY}
STACK {GUID} {ANY}
STACK {GUID} {ANY}
STACK {GUID} {ANY}
STACK {GUID} {ANY}
ERROR {GUID} testing 1213
STACK {GUID} {ANY}
STACK {GUID} {ANY}
STACK {GUID} {ANY}
STACK {GUID} {ANY}
STACK {GUID} {ANY}
STACK {GUID} {ANY}`

	verifyLaneEvents(t, osl, expected, tc.EventsToString())
}

func setTestPanicHandler(l lane.Lane) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	l.SetPanicHandler(func() {
		wg.Done()
		runtime.Goexit()
	})
	return &wg
}

func TestPanicOsl(t *testing.T) {
	_, osl := testMakeFirstOsl(t)

	wg := setTestPanicHandler(osl)
	go func() {
		osl.Fatal("stop me")
		panic("unreachable")
	}()
	wg.Wait()
}

func TestPanicOslF(t *testing.T) {
	_, osl := testMakeFirstOsl(t)

	wg := setTestPanicHandler(osl)
	go func() {
		osl.Fatalf("stop me")
		panic("unreachable")
	}()
	wg.Wait()
}

func TestPanicOslDerived(t *testing.T) {
	_, osl := testMakeFirstOsl(t)

	wg := setTestPanicHandler(osl)
	osl2 := osl.Derive()
	go func() {
		osl2.Fatal("stop me")
		panic("unreachable")
	}()
	wg.Wait()
}

func TestLogTilFull(t *testing.T) {
	_, osl := testMakeFirstOslEx(t, testMax10|testOffline|testNoIndex)

	var wg sync.WaitGroup
	wg.Add(1)
	fails := 0
	osl.SetEmergencyHandler(func(logBuffer []*OslMessage) { fails = len(logBuffer); wg.Done() })

	for i := 0; i < 11; i++ {
		osl.Info(i)
	}

	wg.Wait()

	stats := osl.Stats()
	if stats.MessagesQueued != 11 {
		t.Error("wrong queue count")
	}
	if stats.MessagesSent != 0 {
		t.Error("wrong sent count")
	}
	if stats.MessagesSentFailed != fails {
		t.Error("wrong failed count")
	}
}

func TestLogBulkError(t *testing.T) {
	_, osl := testMakeFirstOslEx(t, testBulkError)

	var wg sync.WaitGroup
	wg.Add(1)
	fails := 0
	failDetail := false
	osl.SetEmergencyHandler(func(logBuffer []*OslMessage) {
		if len(logBuffer) == 1 && strings.HasSuffix(logBuffer[0].LogMessage, "permission denied") {
			failDetail = true
		} else {
			fails = len(logBuffer)
			wg.Done()
		}
	})

	for i := 0; i < 11; i++ {
		osl.Info(i)
	}

	wg.Wait()

	if !failDetail {
		t.Error("did not see emergency logging of upload error")
	}

	stats := osl.Stats()
	if stats.MessagesQueued != 11 {
		t.Error("wrong queue count")
	}
	if stats.MessagesSent != 0 {
		t.Error("wrong sent count")
	}
	if stats.MessagesSentFailed != fails {
		t.Error("wrong failed count")
	}
}

func TestLogBulkErrorThenSuccess(t *testing.T) {
	tc, osl := testMakeFirstOslEx(t, testBulkError)

	var wg sync.WaitGroup
	wg.Add(1)
	fails := 0
	failDetail := false
	osl.SetEmergencyHandler(func(logBuffer []*OslMessage) {
		if len(logBuffer) == 1 && strings.HasSuffix(logBuffer[0].LogMessage, "permission denied") {
			failDetail = true
		} else {
			fails = len(logBuffer)
			wg.Done()
		}
	})

	for i := 0; i < 11; i++ {
		osl.Info(i)
	}

	wg.Wait()

	if !failDetail {
		t.Error("did not see emergency logging of upload error")
	}

	stats := osl.Stats()
	if stats.MessagesQueued != 11 {
		t.Error("wrong queue count")
	}
	if stats.MessagesSent != 0 {
		t.Error("wrong sent count")
	}
	if stats.MessagesSentFailed != fails {
		t.Error("wrong failed count")
	}

	tc.failure = nil

	for i := 0; i < 11; i++ {
		osl.Warn(i)
	}

	tc.waitForBulk(11)

	stats = osl.Stats()
	if stats.MessagesQueued != 22 {
		t.Error("wrong queue count")
	}
	if stats.MessagesSent != 11 {
		t.Error("wrong sent count")
	}
	if stats.MessagesSentFailed != fails {
		t.Error("wrong failed count")
	}
}

func TestLogNoDelay(t *testing.T) {
	_, osl := testMakeFirstOslEx(t, testSlow)

	osl.Info(0)
	for {
		stats := osl.Stats()
		if stats.MessagesSent == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	start := time.Now()
	for i := 0; i < 11; i++ {
		osl.Info(i)
	}
	delta := time.Since(start)
	if delta > time.Millisecond*100 {
		t.Fatalf("too slow: %dms", delta.Milliseconds())
	}

	for {
		stats := osl.Stats()
		if stats.MessagesSent == 12 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// it should have taken two Bulk sends to process everything
	delta = time.Since(start)
	if delta < time.Millisecond*500 {
		t.Fatalf("too fast: %dms", delta.Milliseconds())
	}
}

func TestLaneMetadata(t *testing.T) {
	_, osl := testMakeFirstOsl(t)
	tl := lane.NewTestingLane(context.Background())
	osl.AddTee(tl)

	if tl.GetMetadata("test") != "" {
		t.Fatal("metadata should not be present yet")
	}

	osl.SetMetadata("test", "pass")
	if tl.GetMetadata("test") != "pass" {
		t.Fatal("metadata should be reflected in the tee")
	}

	// generic interface works also
	osl.SetMetadata("test2", "also pass")
	if tl.GetMetadata("test2") != "also pass" {
		t.Fatal("metadata 2 should be reflected in the tee")
	}

	p := osl.(*openSearchLane)
	if p.LogLane.GetMetadata("test") != "pass" || p.GetMetadata("test2") != "also pass" {
		t.Fatal("metadata should exist in the osl")
	}
}

func TestHeavyLogging(t *testing.T) {
	_, osl := testMakeFirstOslEx(t, testNoTees|testSlow)

	var wgs []*sync.WaitGroup
	for task := range 5 {
		var wg sync.WaitGroup
		wg.Add(1)
		wgs = append(wgs, &wg)
		go func() {
			defer wg.Done()
			for msg := range 100 {
				osl.Infof("task %d message %d", task, msg)
				time.Sleep(time.Microsecond)
			}
		}()
	}

	for _, wg := range wgs {
		wg.Wait()
	}

	// no deadlock
}

func TestFinalLog(t *testing.T) {
	tc, osl := testMakeFirstOsl(t)
	deriveL := osl.Derive()

	deriveL.Info("derive test")
	deriveL.Close()

	osl.Info("osl test")

	// is final
	osl.Close()

	expected := `INFO	derive test
INFO	osl test`

	if !tc.VerifyReceived(expected) {
		t.Errorf("Test events don't match")
	}

	stats := osl.Stats()

	if stats.MessagesSent != 2 {
		t.Fatal("expected messages were not sent on final")
	}

}

func TestFinalLogEmergencyWrite(t *testing.T) {
	_, osl := testMakeFirstOslEx(t, testBulkError)

	var expectedWrite []string
	osl.SetEmergencyHandler(func(messages []*OslMessage) {
		for _, message := range messages {
			expectedWrite = append(expectedWrite, message.LogMessage)
		}
	})

	deriveL := osl.Derive()

	deriveL.Info("derive test")
	deriveL.Close()
	osl.Info("osl test")

	// is final
	osl.Close()

	if len(expectedWrite) != 3 {
		t.Fatal("expected messages were not send on emergencyHandler")
	}

}

func TestSharding(t *testing.T) {
	tc, osl := testMakeFirstOsl(t)

	osl.SetIndexSharder(func(baseName string) string { return baseName + "-123" })
	osl.Info(100)

	tc.waitForBulk(1)

	stats := osl.Stats()
	if stats.MessagesQueued != 1 {
		t.Error("wrong queue count")
	}
	if stats.MessagesSent != 1 {
		t.Error("wrong sent count")
	}

	if len(tc.indicies) != 1 {
		t.Error("wrong indicies count")
	} else if tc.indicies[0] != "testing-123" {
		t.Errorf("wrong index: %s", tc.indicies[0])
	}
}

func TestSharding2(t *testing.T) {
	tc, osl := testMakeFirstOsl(t)

	count := 0
	osl.SetIndexSharder(func(baseName string) string { count++; return fmt.Sprintf("%s-%d", baseName, count) })
	osl.Info(100)
	osl.Info(101)

	tc.waitForBulk(2)

	stats := osl.Stats()
	if stats.MessagesQueued != 2 {
		t.Error("wrong queue count")
	}
	if stats.MessagesSent != 2 {
		t.Error("wrong sent count")
	}

	if len(tc.indicies) != 2 {
		t.Error("wrong indicies count")
	} else if tc.indicies[0] != "testing-1" {
		t.Errorf("wrong index 1: %s", tc.indicies[0])
	} else if tc.indicies[1] != "testing-2" {
		t.Errorf("wrong index 2: %s", tc.indicies[1])
	}
}

func TestIndexRequired(t *testing.T) {
	tc := &testClient{}
	tc.install(t)
	cfg := OslConfig{
		OpenSearchHost:      "localhost",
		OpenSearchPort:      1000,
		OpenSearchTransport: &http.Transport{},
		OpenSearchIndex:     "",
	}
	_, err := NewOpenSearchLane(context.Background(), &cfg)
	if !errors.Is(err, ErrIndexNameRequired) {
		t.Fatal("expected error")
	}
}

func TestNilConfig(t *testing.T) {
	osl, err := NewOpenSearchLane(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	osl.Info("test")
}
