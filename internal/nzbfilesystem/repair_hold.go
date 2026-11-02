package nzbfilesystem

import (
	"context"
	"log/slog"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/pkg/rclonecli"
)

// RepairActions is what a repair hold needs from the PAR2 repair service.
type RepairActions interface {
	RepairEnqueuer
	// RepairActive reports whether a repair covering filePath is queued or running.
	RepairActive(ctx context.Context, filePath string) (bool, error)
	// CancelRepair stops the repair covering filePath, if there is one.
	CancelRepair(ctx context.Context, filePath string) error
}

// holdHealthStore is the health record access a repair hold needs.
type holdHealthStore interface {
	GetFileHealth(ctx context.Context, filePath string) (*database.FileHealth, error)
	UpdateFileHealthScheduled(ctx context.Context, filePath string, status database.HealthStatus, errorMessage *string, sourceNzbPath *string, errorDetails *string, noRetry bool, scheduledAt time.Time) error
}

// holdMetadataStore is the metadata access a repair hold needs.
type holdMetadataStore interface {
	UpdateFileStatus(virtualPath string, status metapb.FileStatus) error
	MoveToCorrupted(ctx context.Context, virtualPath string) error
}

const (
	// repairHoldPoll is how often a hold checks on its repair.
	repairHoldPoll = time.Second
	// replacedCooldown is how long a replaced file is not held for again. A
	// reader still on the old copy fails at once rather than starting over.
	replacedCooldown = 30 * time.Minute
	// readSessionIdle ends a read session. It outlasts the longest hold, since a
	// held stream makes no progress but is still the same stream.
	readSessionIdle = 40 * time.Minute
	// readSessionMaxJump is how far past its furthest read a stream may move and
	// still count as the same stream. Anything further is a seek.
	readSessionMaxJump = 256 << 20
)

// repairHolds decides what happens to a read that hit a bad article when
// par2_repair.read_failure_action asks for something other than the default.
//
// A hold outlives any one reader. rclone gives up on a silent request after its
// --timeout and asks again, so one stalled article is many handles in a row.
// State is therefore kept per file, and the repair and its deadline run on their
// own goroutine rather than on whichever reader happened to arrive first.
type repairHolds struct {
	repairs   RepairActions // nil when PAR2 repair isn't wired
	patches   PatchSource   // nil when PAR2 repair isn't wired
	health    holdHealthStore
	metadata  holdMetadataStore
	coalescer *RepairCoalescer
	config    config.ConfigGetter
	poll      time.Duration
	now       func() time.Time

	mu       sync.Mutex
	active   map[string]*fileHold
	replaced map[string]time.Time
	sessions map[string]*readSession
}

// fileHold is the repair a file's readers are waiting on.
type fileHold struct {
	deadline time.Time
	segments map[string]bool // every article waiting on this repair
	// late lists articles that failed after the repair had started, which it
	// may not know about.
	late []string
	done chan struct{}
}

// readSession is one stream through a file: where it started and how far it has
// asked for so far.
type readSession struct {
	start, furthest int64
	lastRead        time.Time
}

// failedRead describes a read that hit a bad article.
type failedRead struct {
	name          string
	segmentID     string
	offset        int64 // file offset of the bad article
	rcloneClient  bool  // the read came from rclone, whose cache gives it lead
	cause         string
	sourceNzbPath string
}

func newRepairHolds(repairs RepairActions, patches PatchSource, health holdHealthStore, metadata holdMetadataStore, coalescer *RepairCoalescer, configGetter config.ConfigGetter) *repairHolds {
	return &repairHolds{
		repairs:   repairs,
		patches:   patches,
		health:    health,
		metadata:  metadata,
		coalescer: coalescer,
		config:    configGetter,
		poll:      repairHoldPoll,
		now:       time.Now,
		active:    make(map[string]*fileHold),
		replaced:  make(map[string]time.Time),
		sessions:  make(map[string]*readSession),
	}
}

// noteRead records a read's offset against the file's current stream.
func (h *repairHolds) noteRead(name string, offset int64) {
	now := h.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.sessions[name]
	if s == nil || now.Sub(s.lastRead) > readSessionIdle || offset < s.start || offset > s.furthest+readSessionMaxJump {
		if len(h.sessions) > 4096 {
			h.pruneSessionsLocked(now)
		}
		h.sessions[name] = &readSession{start: offset, furthest: offset, lastRead: now}
		return
	}
	s.furthest = max(s.furthest, offset)
	s.lastRead = now
}

func (h *repairHolds) pruneSessionsLocked(now time.Time) {
	for name, s := range h.sessions {
		if now.Sub(s.lastRead) > readSessionIdle {
			delete(h.sessions, name)
		}
	}
}

// leadLocked estimates how far the bad article is ahead of the player. rclone's
// first request starts at the player's position and it stays at most a
// read-ahead in front, so both bounds hold. The estimate runs high only if the
// player moved a long way before rclone reached the article, which is rare:
// rclone downloads far faster than playback, so it finds a bad article either
// soon after the stream starts or once it is a full read-ahead in front.
func (h *repairHolds) leadLocked(name string, offset, readAhead int64) int64 {
	s := h.sessions[name]
	if s == nil || offset <= s.start {
		return 0
	}
	return min(offset-s.start, readAhead)
}

// handle applies the configured action to a failed read. It returns true when a
// repaired payload for the article is now available, and false when the read
// should fail, in which case the file has been handed over for replacement.
func (h *repairHolds) handle(ctx context.Context, r failedRead) bool {
	if h.hasPatch(r.segmentID) {
		return true
	}
	cfg := h.config()
	// These settings have their own handling in the normal failure path.
	if !cfg.GetHealthEnabled() || cfg.GetHealthDeleteOnCorruption() {
		return false
	}

	if reason := h.holdRefusal(cfg, r); reason != "" {
		slog.InfoContext(ctx, "Replacing file instead of repairing after a read failure",
			"file", r.name, "segment_id", r.segmentID, "reason", reason)
		h.replace(r)
		return false
	}

	h.mu.Lock()
	if at, ok := h.replaced[r.name]; ok && h.now().Sub(at) < replacedCooldown {
		h.mu.Unlock()
		return false
	}
	hold := h.active[r.name]
	if hold == nil {
		readAhead := cfg.RClone.ReadAheadBytes()
		minLead := cfg.Par2Repair.EffectiveReadFailureMinLeadBytes()
		lead := h.leadLocked(r.name, r.offset, readAhead)
		if lead < minLead {
			h.mu.Unlock()
			slog.InfoContext(ctx, "Replacing file: not enough lead to repair before the player arrives",
				"file", r.name, "segment_id", r.segmentID, "lead_bytes", lead, "min_lead_bytes", minLead)
			h.replace(r)
			return false
		}
		wait := cfg.Par2Repair.EffectiveReadFailureMaxWait()
		hold = &fileHold{
			deadline: h.now().Add(wait),
			segments: map[string]bool{r.segmentID: true},
			done:     make(chan struct{}),
		}
		h.active[r.name] = hold
		h.mu.Unlock()
		slog.InfoContext(ctx, "Holding read while PAR2 repairs a bad article",
			"file", r.name, "segment_id", r.segmentID, "lead_bytes", lead, "max_wait", wait)
		h.repairs.Enqueue(context.WithoutCancel(ctx), r.name, r.segmentID)
		go h.run(r, hold)
	} else {
		if !hold.segments[r.segmentID] {
			hold.segments[r.segmentID] = true
			hold.late = append(hold.late, r.segmentID)
		}
		h.mu.Unlock()
	}

	select {
	case <-ctx.Done():
		// This reader went away. The repair carries on for the next one.
		return false
	case <-hold.done:
		return h.hasPatch(r.segmentID)
	}
}

// holdRefusal says why this read can't be held, or "" if it can.
func (h *repairHolds) holdRefusal(cfg *config.Config, r failedRead) string {
	switch {
	case cfg.Par2Repair.EffectiveReadFailureAction() != config.ReadFailureRepairFirst:
		return "read_failure_action is replace"
	case h.repairs == nil:
		return "PAR2 repair is not available"
	case cfg.Par2Repair.Enabled == nil || !*cfg.Par2Repair.Enabled:
		return "PAR2 repair is disabled"
	case cfg.MountType != config.MountTypeRClone || !strings.EqualFold(cfg.RClone.VFSCacheMode, "full"):
		return "repair first needs the built-in rclone mount in full cache mode"
	case !r.rcloneClient:
		return "the read did not come through rclone, so it has no lead"
	}
	return ""
}

// run watches the repair until every waiting article is rebuilt, the repair
// ends without them, or the deadline passes.
func (h *repairHolds) run(r failedRead, hold *fileHold) {
	defer func() {
		h.mu.Lock()
		delete(h.active, r.name)
		h.mu.Unlock()
		close(hold.done)
	}()
	ctx := context.Background()
	ticker := time.NewTicker(h.poll)
	defer ticker.Stop()
	for range ticker.C {
		h.mu.Lock()
		missing := 0
		for id := range hold.segments {
			if !h.hasPatch(id) {
				missing++
			}
		}
		h.mu.Unlock()
		if missing == 0 {
			slog.InfoContext(ctx, "PAR2 repair finished in time, releasing held reads", "file", r.name)
			return
		}

		if !h.now().Before(hold.deadline) {
			slog.WarnContext(ctx, "PAR2 repair ran out of time, cancelling it and replacing the file", "file", r.name)
			cancelCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if err := h.repairs.CancelRepair(cancelCtx, r.name); err != nil {
				// Cancelling marks the job first, so its outcome is ignored even
				// if it is slow to stop. Replacing is still right.
				slog.WarnContext(ctx, "PAR2 repair did not stop cleanly", "file", r.name, "error", err)
			}
			cancel()
			// It may have finished just before the cancel landed.
			if h.allPatched(hold) {
				slog.InfoContext(ctx, "PAR2 repair finished as it was being cancelled, keeping the file", "file", r.name)
				return
			}
			h.replace(r)
			return
		}

		active, err := h.repairs.RepairActive(ctx, r.name)
		if err != nil {
			slog.WarnContext(ctx, "Could not check on PAR2 repair", "file", r.name, "error", err)
			continue
		}
		if active {
			continue
		}
		// The repair is over. Articles that failed after it started may be
		// outside what it set out to fix, so they get a repair of their own.
		h.mu.Lock()
		late := hold.late
		hold.late = nil
		h.mu.Unlock()
		if retried := h.requeueUnpatched(ctx, r.name, late); retried {
			continue
		}
		if h.allPatched(hold) {
			return
		}
		slog.WarnContext(ctx, "PAR2 repair could not rebuild the file, replacing it", "file", r.name)
		h.replace(r)
		return
	}
}

func (h *repairHolds) requeueUnpatched(ctx context.Context, name string, late []string) bool {
	retried := false
	for _, id := range late {
		if !h.hasPatch(id) {
			h.repairs.Enqueue(ctx, name, id)
			retried = true
		}
	}
	return retried
}

func (h *repairHolds) allPatched(hold *fileHold) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id := range hold.segments {
		if !h.hasPatch(id) {
			return false
		}
	}
	return true
}

func (h *repairHolds) hasPatch(segmentID string) bool {
	if h.patches == nil {
		return false
	}
	_, ok := h.patches.Get(strings.Trim(segmentID, "<>"))
	return ok
}

// replace hands the file to the health worker to fetch a new release. It spends
// the file's repair debounce token, so readers failing on the same file don't
// repeat this through the normal failure path.
func (h *repairHolds) replace(r failedRead) {
	if !h.coalescer.ShouldTrigger(r.name) {
		return
	}
	h.mu.Lock()
	h.replaced[r.name] = h.now()
	h.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := h.config()
	if err := h.metadata.UpdateFileStatus(r.name, metapb.FileStatus_FILE_STATUS_CORRUPTED); err != nil {
		slog.WarnContext(ctx, "Failed to mark file corrupted for replacement", "file", r.name, "error", err)
	}

	// With a library path, moving the metadata makes the ARR rescan see the file
	// as gone. This handle's reads fail either way, and the debounce keeps others
	// from repeating it.
	if health, err := h.health.GetFileHealth(ctx, r.name); err == nil && health != nil && health.LibraryPath != nil && *health.LibraryPath != "" {
		relative := strings.TrimPrefix(strings.TrimPrefix(r.name, cfg.MountPath), "/")
		if err := h.metadata.MoveToCorrupted(ctx, relative); err == nil {
			h.coalescer.MarkRemoved(r.name)
			h.coalescer.EnqueueRefresh(path.Dir(rclonecli.ToVFSPath(r.name)))
		} else {
			slog.WarnContext(ctx, "Failed to move metadata for replacement", "file", r.name, "error", err)
		}
	}

	cause := r.cause
	var sourceNzb *string
	if r.sourceNzbPath != "" {
		sourceNzb = &r.sourceNzbPath
	}
	if err := h.health.UpdateFileHealthScheduled(ctx, r.name, database.HealthStatusRepairTriggered, &cause, sourceNzb, nil, true, h.now().UTC()); err != nil {
		slog.WarnContext(ctx, "Failed to schedule replacement", "file", r.name, "error", err)
	}
}

type rcloneClientKey struct{}

// WithRcloneClient marks ctx as serving a request from rclone, whose read-ahead
// gives a repair time to finish before the player arrives.
func WithRcloneClient(ctx context.Context) context.Context {
	return context.WithValue(ctx, rcloneClientKey{}, true)
}

func isRcloneClient(ctx context.Context) bool {
	v, _ := ctx.Value(rcloneClientKey{}).(bool)
	return v
}
