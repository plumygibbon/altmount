package nzbfilesystem

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

const holdTestFile = "complete/TV/show.mkv"

type fakeRepairs struct {
	mu       sync.Mutex
	active   bool
	enqueued []string
	cancels  int
	onCancel func()
	cancelEr error
}

func (f *fakeRepairs) Enqueue(_ context.Context, _ string, seg string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enqueued = append(f.enqueued, seg)
	f.active = true
}

func (f *fakeRepairs) RepairActive(context.Context, string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active, nil
}

func (f *fakeRepairs) CancelRepair(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancels++
	f.active = false
	if f.onCancel != nil {
		f.onCancel()
	}
	return f.cancelEr
}

func (f *fakeRepairs) finish() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active = false
}

func (f *fakeRepairs) state() ([]string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.enqueued...), f.cancels
}

type fakePatches struct {
	mu sync.Mutex
	m  map[string][]byte
}

func (f *fakePatches) Get(id string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.m[id]
	return p, ok
}

func (f *fakePatches) add(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[id] = []byte{1}
}

type fakeHoldHealth struct {
	mu       sync.Mutex
	library  string
	statuses []database.HealthStatus
}

func (f *fakeHoldHealth) GetFileHealth(context.Context, string) (*database.FileHealth, error) {
	if f.library == "" {
		return nil, nil
	}
	return &database.FileHealth{LibraryPath: &f.library}, nil
}

func (f *fakeHoldHealth) UpdateFileHealthScheduled(_ context.Context, _ string, status database.HealthStatus, _ *string, _ *string, _ *string, _ bool, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses = append(f.statuses, status)
	return nil
}

func (f *fakeHoldHealth) replacements() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, s := range f.statuses {
		if s == database.HealthStatusRepairTriggered {
			n++
		}
	}
	return n
}

type fakeHoldMeta struct {
	mu    sync.Mutex
	moved int
}

func (f *fakeHoldMeta) UpdateFileStatus(string, metapb.FileStatus) error { return nil }
func (f *fakeHoldMeta) MoveToCorrupted(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.moved++
	return nil
}

type holdFixture struct {
	h       *repairHolds
	repairs *fakeRepairs
	patches *fakePatches
	health  *fakeHoldHealth
	meta    *fakeHoldMeta
	cfg     *config.Config
}

func newHoldFixture(t *testing.T, mutate func(*config.Config)) *holdFixture {
	t.Helper()
	on := true
	cfg := config.DefaultConfig("")
	cfg.MountType = config.MountTypeRClone
	cfg.RClone.VFSCacheMode = "full"
	cfg.RClone.VFSReadAhead = "1G"
	cfg.Par2Repair.Enabled = &on
	cfg.Par2Repair.ReadFailureAction = config.ReadFailureRepairFirst
	cfg.Par2Repair.ReadFailureMaxWaitSeconds = 30
	cfg.Health.Enabled = &on
	if mutate != nil {
		mutate(cfg)
	}
	f := &holdFixture{
		repairs: &fakeRepairs{},
		patches: &fakePatches{m: map[string][]byte{}},
		health:  &fakeHoldHealth{},
		meta:    &fakeHoldMeta{},
		cfg:     cfg,
	}
	f.h = newRepairHolds(f.repairs, f.patches, f.health, f.meta, nil, func() *config.Config { return cfg })
	f.h.poll = 2 * time.Millisecond
	return f
}

// deepRead is a failed read well past the minimum lead from a stream at offset 0.
func deepRead(seg string) failedRead {
	return failedRead{name: holdTestFile, segmentID: seg, offset: 2 << 30, rcloneClient: true, cause: "boom"}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHoldReleasesReadOnceRepaired(t *testing.T) {
	f := newHoldFixture(t, nil)
	f.h.noteRead(holdTestFile, 0)

	result := make(chan bool, 1)
	go func() { result <- f.h.handle(context.Background(), deepRead("a")) }()
	waitFor(t, "the repair to be queued", func() bool { e, _ := f.repairs.state(); return len(e) == 1 })

	select {
	case <-result:
		t.Fatal("read released before the repair finished")
	case <-time.After(20 * time.Millisecond):
	}
	f.patches.add("a")
	if !<-result {
		t.Fatal("handle = false, want the repaired read released")
	}
	if n := f.health.replacements(); n != 0 {
		t.Fatalf("replacements = %d, want 0", n)
	}
}

// rclone gives up on a silent request and asks again. That must rejoin the same
// repair and the same deadline, not start over.
func TestHoldSurvivesReconnects(t *testing.T) {
	f := newHoldFixture(t, nil)
	f.h.noteRead(holdTestFile, 0)

	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan bool, 1)
	go func() { first <- f.h.handle(ctx, deepRead("a")) }()
	waitFor(t, "the repair to be queued", func() bool { e, _ := f.repairs.state(); return len(e) == 1 })
	f.h.mu.Lock()
	deadline := f.h.active[holdTestFile].deadline
	f.h.mu.Unlock()

	cancel()
	if <-first {
		t.Fatal("a reader that went away got a repaired result")
	}
	if n := f.health.replacements(); n != 0 {
		t.Fatalf("a reader going away replaced the file: %d", n)
	}

	// The reconnected stream starts at the bad article itself. It's still one
	// stream, so it must not look like a stream with no lead.
	f.h.noteRead(holdTestFile, 2<<30)
	second := make(chan bool, 1)
	go func() { second <- f.h.handle(context.Background(), deepRead("a")) }()
	time.Sleep(10 * time.Millisecond)

	f.h.mu.Lock()
	hold := f.h.active[holdTestFile]
	f.h.mu.Unlock()
	if hold == nil || !hold.deadline.Equal(deadline) {
		t.Fatal("reconnect did not rejoin the running hold")
	}
	if e, _ := f.repairs.state(); len(e) != 1 {
		t.Fatalf("enqueued %v, want one repair", e)
	}
	f.patches.add("a")
	if !<-second {
		t.Fatal("reconnected read was not released")
	}
}

func TestHoldTimesOutThenCancelsBeforeReplacing(t *testing.T) {
	f := newHoldFixture(t, func(c *config.Config) { c.Par2Repair.ReadFailureMaxWaitSeconds = 1 })
	f.h.noteRead(holdTestFile, 0)
	var replacedAtCancel int
	f.repairs.onCancel = func() { replacedAtCancel = f.health.replacements() }

	if f.h.handle(context.Background(), deepRead("a")) {
		t.Fatal("handle = true after the deadline")
	}
	if _, cancels := f.repairs.state(); cancels != 1 {
		t.Fatalf("cancels = %d, want 1", cancels)
	}
	if replacedAtCancel != 0 {
		t.Fatal("replaced before the repair was cancelled")
	}
	if n := f.health.replacements(); n != 1 {
		t.Fatalf("replacements = %d, want 1", n)
	}
}

// A repair that finishes just as it's being cancelled keeps the file.
func TestHoldKeepsFileWhenRepairBeatsTheCancel(t *testing.T) {
	f := newHoldFixture(t, func(c *config.Config) { c.Par2Repair.ReadFailureMaxWaitSeconds = 1 })
	f.h.noteRead(holdTestFile, 0)
	f.repairs.onCancel = func() { f.patches.add("a") }
	f.repairs.cancelEr = errors.New("job not found")

	if !f.h.handle(context.Background(), deepRead("a")) {
		t.Fatal("handle = false, want the finished repair kept")
	}
	if n := f.health.replacements(); n != 0 {
		t.Fatalf("replacements = %d, want 0", n)
	}
}

// A cancel that doesn't stop in time must still end in a replacement.
func TestHoldReplacesEvenIfCancelFails(t *testing.T) {
	f := newHoldFixture(t, func(c *config.Config) { c.Par2Repair.ReadFailureMaxWaitSeconds = 1 })
	f.h.noteRead(holdTestFile, 0)
	f.repairs.cancelEr = errors.New("did not stop in time")

	if f.h.handle(context.Background(), deepRead("a")) {
		t.Fatal("handle = true, want the read failed")
	}
	if n := f.health.replacements(); n != 1 {
		t.Fatalf("replacements = %d, want 1", n)
	}
}

func TestHoldReplacesWhenRepairFails(t *testing.T) {
	f := newHoldFixture(t, nil)
	f.h.noteRead(holdTestFile, 0)
	result := make(chan bool, 1)
	go func() { result <- f.h.handle(context.Background(), deepRead("a")) }()
	waitFor(t, "the repair to be queued", func() bool { e, _ := f.repairs.state(); return len(e) == 1 })
	f.repairs.finish()
	if <-result {
		t.Fatal("handle = true, want the read failed")
	}
	if n := f.health.replacements(); n != 1 {
		t.Fatalf("replacements = %d, want 1", n)
	}
	if _, cancels := f.repairs.state(); cancels != 0 {
		t.Fatalf("cancels = %d, want 0: the repair had already ended", cancels)
	}
}

// With nobody reading, a failed repair must still lead to a replacement rather
// than leave the file stuck.
func TestHoldReplacesWithNoReaderLeft(t *testing.T) {
	f := newHoldFixture(t, nil)
	f.h.noteRead(holdTestFile, 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.h.handle(ctx, deepRead("a")); close(done) }()
	waitFor(t, "the repair to be queued", func() bool { e, _ := f.repairs.state(); return len(e) == 1 })
	cancel()
	<-done
	f.repairs.finish()
	waitFor(t, "the replacement", func() bool { return f.health.replacements() == 1 })
}

func TestHoldRefusals(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*config.Config)
		read   func(failedRead) failedRead
	}{
		{name: "replace action", mutate: func(c *config.Config) { c.Par2Repair.ReadFailureAction = config.ReadFailureReplace }},
		{name: "repair disabled", mutate: func(c *config.Config) { off := false; c.Par2Repair.Enabled = &off }},
		{name: "not rclone mount", mutate: func(c *config.Config) { c.MountType = config.MountTypeFuse }},
		{name: "not full cache", mutate: func(c *config.Config) { c.RClone.VFSCacheMode = "writes" }},
		{name: "not from rclone", read: func(r failedRead) failedRead { r.rcloneClient = false; return r }},
		{name: "too close to stream start", read: func(r failedRead) failedRead { r.offset = 100 << 20; return r }},
		{name: "lead above read-ahead", mutate: func(c *config.Config) { c.RClone.VFSReadAhead = "512M" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newHoldFixture(t, tt.mutate)
			f.h.noteRead(holdTestFile, 0)
			r := deepRead("a")
			if tt.read != nil {
				r = tt.read(r)
			}
			if f.h.handle(context.Background(), r) {
				t.Fatal("handle = true, want the read failed")
			}
			if n := f.health.replacements(); n != 1 {
				t.Fatalf("replacements = %d, want 1", n)
			}
			if e, _ := f.repairs.state(); len(e) != 0 {
				t.Fatalf("repair queued %v, want none", e)
			}
		})
	}
}

// With health off, or delete-on-corruption, the normal failure path owns this.
func TestHoldLeavesOtherPoliciesAlone(t *testing.T) {
	off := false
	for name, mutate := range map[string]func(*config.Config){
		"health disabled": func(c *config.Config) { c.Health.Enabled = &off },
		"delete on corruption": func(c *config.Config) {
			c.Health.CorruptionAction = "delete"
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newHoldFixture(t, mutate)
			f.h.noteRead(holdTestFile, 0)
			if f.h.handle(context.Background(), deepRead("a")) {
				t.Fatal("handle = true")
			}
			if n := f.health.replacements(); n != 0 {
				t.Fatalf("replacements = %d, want 0", n)
			}
		})
	}
}

func TestHoldServesExistingPatchWithoutAnyAction(t *testing.T) {
	f := newHoldFixture(t, nil)
	f.patches.add("a")
	if !f.h.handle(context.Background(), failedRead{name: holdTestFile, segmentID: "<a>", rcloneClient: true}) {
		t.Fatal("handle = false with a patch available")
	}
	if e, _ := f.repairs.state(); len(e) != 0 {
		t.Fatalf("queued %v", e)
	}
}

// Once replaced, a file isn't held again, and repeated failures replace it once.
func TestHoldReplacesOnlyOnce(t *testing.T) {
	coalesced := NewRepairCoalescer(nil, func() *config.Config { return config.DefaultConfig("") })
	t.Cleanup(coalesced.Close)
	f := newHoldFixture(t, nil)
	f.h.coalescer = coalesced
	f.h.noteRead(holdTestFile, 0)
	f.repairs.finish()
	r := deepRead("a")
	r.offset = 100 << 20
	f.h.handle(context.Background(), r)
	f.h.noteRead(holdTestFile, 0)
	if f.h.handle(context.Background(), deepRead("b")) {
		t.Fatal("handle = true on a replaced file")
	}
	if n := f.health.replacements(); n != 1 {
		t.Fatalf("replacements = %d, want 1", n)
	}
	if e, _ := f.repairs.state(); len(e) != 0 {
		t.Fatalf("held a replaced file: queued %v", e)
	}
}

// An article that fails after the repair began may be outside what it's fixing.
func TestHoldRepairsArticleFoundAfterRepairStarted(t *testing.T) {
	f := newHoldFixture(t, nil)
	f.h.noteRead(holdTestFile, 0)
	results := make(chan bool, 2)
	go func() { results <- f.h.handle(context.Background(), deepRead("a")) }()
	waitFor(t, "the repair to be queued", func() bool { e, _ := f.repairs.state(); return len(e) == 1 })
	go func() { results <- f.h.handle(context.Background(), deepRead("b")) }()
	waitFor(t, "the late article to join", func() bool {
		f.h.mu.Lock()
		defer f.h.mu.Unlock()
		hold := f.h.active[holdTestFile]
		return hold != nil && hold.segments["b"]
	})

	f.patches.add("a")
	f.repairs.finish()
	waitFor(t, "a second repair for the late article", func() bool { e, _ := f.repairs.state(); return len(e) == 2 })
	f.patches.add("b")
	if !<-results || !<-results {
		t.Fatal("a held read failed")
	}
	if n := f.health.replacements(); n != 0 {
		t.Fatalf("replacements = %d, want 0", n)
	}
}

func TestReadSessions(t *testing.T) {
	f := newHoldFixture(t, nil)
	const ra = int64(1 << 30)
	lead := func(off int64) int64 {
		f.h.mu.Lock()
		defer f.h.mu.Unlock()
		return f.h.leadLocked(holdTestFile, off, ra)
	}
	if got := lead(5 << 20); got != 0 {
		t.Fatalf("no session: lead = %d", got)
	}
	f.h.noteRead(holdTestFile, 100<<20)
	if got := lead(600 << 20); got != 500<<20 {
		t.Fatalf("lead = %d, want 500MiB", got)
	}
	// Capped at the read-ahead: rclone reads no further ahead of the player.
	if got := lead(5 << 30); got != ra {
		t.Fatalf("lead = %d, want the read-ahead", got)
	}
	// Reads moving forward are the same stream.
	f.h.noteRead(holdTestFile, 200<<20)
	if got := lead(600 << 20); got != 500<<20 {
		t.Fatalf("sequential read moved the start: lead = %d", got)
	}
	// A big jump forward is a seek, and the player is there now.
	f.h.noteRead(holdTestFile, 3<<30)
	if got := lead(3<<30 + 50<<20); got != 50<<20 {
		t.Fatalf("seek forward: lead = %d, want 50MiB", got)
	}
	// So is going backwards.
	f.h.noteRead(holdTestFile, 1<<30)
	if got := lead(1<<30 + 10<<20); got != 10<<20 {
		t.Fatalf("seek back: lead = %d, want 10MiB", got)
	}
	// A stream that stopped long ago doesn't count.
	future := time.Now().Add(readSessionIdle + time.Minute)
	f.h.now = func() time.Time { return future }
	f.h.noteRead(holdTestFile, 1<<30+10<<20)
	if got := lead(1<<30 + 20<<20); got != 10<<20 {
		t.Fatalf("idle session kept: lead = %d", got)
	}
}
