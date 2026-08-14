package config

import (
	"strings"
	"testing"
	"time"
)

// Every limit has a finite default. A zero default would be the old
// search.maxRows behaviour -- read as "no cap" and letting one request
// consume the process.
func TestDefaultsAreAllFinite(t *testing.T) {
	d := DefaultLimits()
	for _, f := range fields() {
		if v := f.get(&d); v <= 0 {
			t.Errorf("%s default is %d, want a positive finite value", f.name, v)
		}
	}
	if d.MaxQueryDuration <= 0 {
		t.Errorf("max-query-duration default is %s, want positive", d.MaxQueryDuration)
	}
}

// A zero field means "use the default", so a caller can set one limit
// without restating the rest.
func TestNormalizeFillsZeroFromDefaults(t *testing.T) {
	l := Limits{MaxQueryRows: 7}
	if err := l.Normalize(); err != nil {
		t.Fatal(err)
	}
	if l.MaxQueryRows != 7 {
		t.Errorf("MaxQueryRows %d, want the explicit 7", l.MaxQueryRows)
	}
	d := DefaultLimits()
	if l.MaxBodyBytes != d.MaxBodyBytes {
		t.Errorf("MaxBodyBytes %d, want the default %d", l.MaxBodyBytes, d.MaxBodyBytes)
	}
	if l.MaxQueryDuration != d.MaxQueryDuration {
		t.Errorf("MaxQueryDuration %s, want the default %s", l.MaxQueryDuration, d.MaxQueryDuration)
	}
}

// Unlimited is explicit and survives normalization; any other negative is an
// error rather than being clamped, because -5 is a bug and hiding it defers
// the failure to the moment the limit would have mattered.
func TestNormalizeRejectsNegativesButKeepsUnlimited(t *testing.T) {
	l := Limits{MaxQueryRows: Unlimited}
	if err := l.Normalize(); err != nil {
		t.Fatal(err)
	}
	if l.MaxQueryRows != Unlimited {
		t.Errorf("MaxQueryRows %d, want Unlimited", l.MaxQueryRows)
	}

	for _, c := range []struct {
		name string
		l    Limits
		want string
	}{
		{"rows", Limits{MaxQueryRows: -5}, "max-query-rows"},
		{"body", Limits{MaxBodyBytes: -2}, "max-body-bytes"},
		{"tenants", Limits{MaxOpenTenants: -3}, "max-open-tenants"},
		{"duration", Limits{MaxQueryDuration: -2 * time.Second}, "max-query-duration"},
	} {
		l := c.l
		err := l.Normalize()
		if err == nil {
			t.Errorf("%s: no error for a negative value", c.name)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %q does not name the field", c.name, err)
		}
	}
}

// A decompressed cap below the body cap would accept a body and then reject
// it for being larger than a limit it could never satisfy.
func TestNormalizeRejectsInconsistentCompressionLimits(t *testing.T) {
	l := Limits{MaxBodyBytes: 1 << 20, MaxDecompressed: 1 << 10}
	err := l.Normalize()
	if err == nil {
		t.Fatal("no error for a decompressed cap below the body cap")
	}
	if !strings.Contains(err.Error(), "max-decompressed-bytes") {
		t.Errorf("error %q does not name the field", err)
	}

	// Unlimited on either side is consistent by construction.
	ok := Limits{MaxBodyBytes: 1 << 20, MaxDecompressed: Unlimited}
	if err := ok.Normalize(); err != nil {
		t.Errorf("unlimited decompressed cap rejected: %v", err)
	}
}

// Normalize is idempotent: running it twice does not drift.
func TestNormalizeIsIdempotent(t *testing.T) {
	l := Limits{MaxQueryRows: 42}
	if err := l.Normalize(); err != nil {
		t.Fatal(err)
	}
	first := l
	if err := l.Normalize(); err != nil {
		t.Fatal(err)
	}
	if l != first {
		t.Fatalf("second Normalize changed the limits:\n %+v\n %+v", first, l)
	}
}

// The test limits stay valid and are genuinely smaller, so a test can reach a
// bound without building a production-sized body.
func TestTestLimitsAreValidAndSmaller(t *testing.T) {
	l := TestLimits()
	if err := l.Normalize(); err != nil {
		t.Fatal(err)
	}
	d := DefaultLimits()
	if l.MaxBodyBytes >= d.MaxBodyBytes || l.MaxQueryRows >= d.MaxQueryRows {
		t.Fatal("TestLimits are not smaller than the defaults")
	}
}

func TestConfigValidate(t *testing.T) {
	c := Default()
	if err := c.Validate(); err == nil {
		t.Fatal("no error for a config with no data directory")
	}
	c.Dir = t.TempDir()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.Limits.MaxQueryRows = -9
	if err := c.Validate(); err == nil {
		t.Fatal("Validate did not normalize the limits")
	}
}
