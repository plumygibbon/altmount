package config

import (
	"testing"
	"time"
)

func TestParseRcloneSize(t *testing.T) {
	tests := map[string]int64{
		"1G": 1 << 30, "1g": 1 << 30, "512M": 512 << 20, "1.5G": 3 << 29, "64K": 64 << 10,
		"2GiB": 2 << 30, "100": 100 << 10, "1T": 1 << 40, "": 0, "off": 0, "-1G": 0,
	}
	for in, want := range tests {
		if got := parseRcloneSize(in); got != want {
			t.Errorf("parseRcloneSize(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestReadFailureDefaults(t *testing.T) {
	var p Par2RepairConfig
	if got := p.EffectiveReadFailureAction(); got != ReadFailureDefault {
		t.Errorf("action = %q", got)
	}
	if got := p.EffectiveReadFailureMinLeadBytes(); got != 1<<30 {
		t.Errorf("min lead = %d", got)
	}
	if got := p.EffectiveReadFailureMaxWait(); got != 5*time.Minute {
		t.Errorf("max wait = %s", got)
	}
	p = Par2RepairConfig{ReadFailureAction: "nonsense", ReadFailureMinLeadMB: 2048, ReadFailureMaxWaitSeconds: 60}
	if got := p.EffectiveReadFailureAction(); got != ReadFailureDefault {
		t.Errorf("unknown action = %q", got)
	}
	if got := p.EffectiveReadFailureMinLeadBytes(); got != 2<<30 {
		t.Errorf("min lead = %d", got)
	}
	if got := p.EffectiveReadFailureMaxWait(); got != time.Minute {
		t.Errorf("max wait = %s", got)
	}
}

func TestDefaultConfigLeavesReadFailureBehaviourAlone(t *testing.T) {
	if got := DefaultConfig("").Par2Repair.EffectiveReadFailureAction(); got != ReadFailureDefault {
		t.Errorf("default action = %q, want the existing behaviour", got)
	}
}
