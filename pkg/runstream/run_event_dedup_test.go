package runstream

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// Test_PublishTFRunEvent_DedupesSameRunIDAndStatus verifies that two
// publishes for the same (runID, newStatus) result in a single message on
// the RUN_EVENTS stream. This is the core fix for duplicate GitLab reply
// comments — TFC fires a webhook for every notification trigger, and several
// triggers can resolve to the same RunStatus over the run's lifecycle.
func Test_PublishTFRunEvent_DedupesSameRunIDAndStatus(t *testing.T) {
	_, url := startTestNATS(t)
	nc := testConnect(t, url)
	defer nc.Close()
	js := testGetJetstreamContext(t, nc)

	s := newTestStream(t, js)
	seedRunMetadata(t, s, "run-DEDUP", "gitlab")

	publishOnce(t, s, "run-DEDUP", "planning")
	publishOnce(t, s, "run-DEDUP", "planning")
	publishOnce(t, s, "run-DEDUP", "planning")

	if got := streamMsgs(t, js); got != 1 {
		t.Fatalf("expected 1 message on %s after 3 same-status publishes, got %d",
			RunEventsStreamName, got)
	}
}

// Test_PublishTFRunEvent_KeepsDistinctStatuses verifies that the dedup is
// scoped to (runID, newStatus). Different status values for the same run
// must all land on the stream — that's how real state transitions reach the
// GitLab reply pipeline.
func Test_PublishTFRunEvent_KeepsDistinctStatuses(t *testing.T) {
	_, url := startTestNATS(t)
	nc := testConnect(t, url)
	defer nc.Close()
	js := testGetJetstreamContext(t, nc)

	s := newTestStream(t, js)
	seedRunMetadata(t, s, "run-DISTINCT", "gitlab")

	for _, status := range []string{"planning", "planned", "applying", "applied"} {
		publishOnce(t, s, "run-DISTINCT", status)
	}

	if got := streamMsgs(t, js); got != 4 {
		t.Fatalf("expected 4 messages on %s for 4 distinct statuses, got %d",
			RunEventsStreamName, got)
	}
}

// Test_PublishTFRunEvent_DedupesAcrossRuns verifies that the dedup is keyed
// to runID so different runs at the same status are not collapsed.
func Test_PublishTFRunEvent_DedupesAcrossRuns(t *testing.T) {
	_, url := startTestNATS(t)
	nc := testConnect(t, url)
	defer nc.Close()
	js := testGetJetstreamContext(t, nc)

	s := newTestStream(t, js)
	seedRunMetadata(t, s, "run-A", "gitlab")
	seedRunMetadata(t, s, "run-B", "gitlab")

	publishOnce(t, s, "run-A", "planning")
	publishOnce(t, s, "run-A", "planning") // dedup
	publishOnce(t, s, "run-B", "planning") // distinct run, must land
	publishOnce(t, s, "run-B", "planning") // dedup

	if got := streamMsgs(t, js); got != 2 {
		t.Fatalf("expected 2 messages (one per runID) on %s, got %d",
			RunEventsStreamName, got)
	}
}

// Test_PublishTFRunEvent_PlanThenApplyFlow simulates the manual
// plan-then-`tfc apply` workflow. Plan and apply are two separate TFC runs
// with distinct runIDs, but both pass through the planning -> planned
// status transitions before the apply run progresses to applying -> applied.
// Each run must own its own set of dedup messages — none collide.
func Test_PublishTFRunEvent_PlanThenApplyFlow(t *testing.T) {
	_, url := startTestNATS(t)
	nc := testConnect(t, url)
	defer nc.Close()
	js := testGetJetstreamContext(t, nc)

	s := newTestStream(t, js)
	seedRunMetadata(t, s, "run-PLAN", "gitlab")
	seedRunMetadata(t, s, "run-APPLY", "gitlab")

	// Plan run: TFC walks pending -> planning -> planned -> planned_and_finished.
	// Each redundant webhook within a status (e.g. plan_queued + planning
	// both surface as `planning`) is collapsed by dedup.
	planStatuses := []string{"pending", "planning", "planning", "planned", "planned_and_finished"}
	for _, status := range planStatuses {
		publishOnce(t, s, "run-PLAN", status)
	}

	// Apply run: separate runID. TFC runs always plan first, so this run
	// also passes through `planning` and `planned` before reaching the apply
	// phase. Same status names, different runID = distinct dedup keys.
	applyStatuses := []string{"pending", "planning", "planned", "applying", "applying", "applied", "applied"}
	for _, status := range applyStatuses {
		publishOnce(t, s, "run-APPLY", status)
	}

	// Expected unique (runID, status) pairs:
	//   run-PLAN:  pending, planning, planned, planned_and_finished   (4)
	//   run-APPLY: pending, planning, planned, applying, applied      (5)
	const want = 9
	if got := streamMsgs(t, js); got != want {
		t.Fatalf("plan+apply flow: expected %d distinct messages, got %d", want, got)
	}
}

// Test_tfRunEventMsgID locks in the exact dedup-key shape so a refactor
// can't accidentally widen or narrow the scope.
func Test_tfRunEventMsgID(t *testing.T) {
	tests := []struct {
		runID, status, want string
	}{
		{"run-rKys4r9a19W7Dk8Q", "planning", "run-rKys4r9a19W7Dk8Q:planning"},
		{"run-rKys4r9a19W7Dk8Q", "applied", "run-rKys4r9a19W7Dk8Q:applied"},
		{"run-OTHER", "planning", "run-OTHER:planning"},
	}
	for _, tc := range tests {
		if got := tfRunEventMsgID(tc.runID, tc.status); got != tc.want {
			t.Errorf("tfRunEventMsgID(%q, %q) = %q, want %q",
				tc.runID, tc.status, got, tc.want)
		}
	}
}

// newTestStream wires up a Stream wired to the test JetStream context. We
// skip NewStream() because it spins up a polling task dispatcher goroutine
// that we don't need for these tests.
//
// JetStream uses on-disk storage by default and natstest reuses os.TempDir()
// across instances, so the stream can survive a NATS server shutdown. Purge
// it on entry so each test starts at zero messages and zero dedup history.
func newTestStream(t *testing.T, js nats.JetStreamContext) *Stream {
	t.Helper()
	configureTFRunEventsStream(js, testDedupWindow)
	if err := js.PurgeStream(RunEventsStreamName); err != nil {
		t.Fatalf("could not purge %s before test: %v", RunEventsStreamName, err)
	}
	kv, err := configureTFRunMetadataKVStore(js)
	if err != nil {
		t.Fatalf("could not configure metadata KV: %v", err)
	}
	return &Stream{js: js, metadataKV: kv}
}

func seedRunMetadata(t *testing.T, s *Stream, runID, vcs string) {
	t.Helper()
	// KV is persistent across test runs (file storage under os.TempDir).
	// Clear any prior entry so AddRunMeta's Create() doesn't fail with
	// "key exists" when tests are re-run.
	_ = s.metadataKV.Delete(runID)
	rmd := &TFRunMetadata{
		RunID:                                runID,
		Organization:                         "zapier",
		Workspace:                            "test-ws",
		Action:                               "plan",
		CommitSHA:                            "deadbeef",
		MergeRequestProjectNameWithNamespace: "zapier/terraform-services",
		MergeRequestIID:                      1,
		VcsProvider:                          vcs,
	}
	if err := s.AddRunMeta(rmd); err != nil {
		t.Fatalf("could not seed RunMetadata for %q: %v", runID, err)
	}
}

func publishOnce(t *testing.T, s *Stream, runID, status string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	re := &TFRunEvent{
		RunID:        runID,
		Organization: "zapier",
		Workspace:    "test-ws",
		NewStatus:    status,
	}
	if err := s.PublishTFRunEvent(ctx, re); err != nil {
		t.Fatalf("PublishTFRunEvent(%q, %q) error: %v", runID, status, err)
	}
}

func streamMsgs(t *testing.T, js nats.JetStreamContext) uint64 {
	t.Helper()
	info, err := js.StreamInfo(RunEventsStreamName)
	if err != nil {
		t.Fatalf("could not read %s info: %v", RunEventsStreamName, err)
	}
	return info.State.Msgs
}
