package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// One corrupt group among healthy ones.
//
// The shape that matters is not "a corrupt store" -- it is a store where
// almost everything is fine. A policy that only knows how to refuse leaves an
// operator with 9,999 readable groups and no way to read them; a policy that
// only knows how to skip answers every query touching that range with fewer
// rows and says nothing. Both behaviours exist here, and which one is in force
// is the operator's decision, not this package's.

// corruptStore builds a store with n groups and then corrupts the middle one
// on disk. It returns the directory and the id of the corrupted group.
func corruptStore(t *testing.T, n int) (dir string, badID uint64) {
	t.Helper()
	dir = t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	var ids []uint64
	for b := 0; b < n; b++ {
		id, err := st.AppendGroup(crashGroup(b))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	st.Close()

	badID = ids[n/2]
	path := filepath.Join(dir, groupFileName(badID))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip bits in the body. The trailer holds the checksum, so this is the
	// shape a bad sector produces: the file is the right length, the framing
	// still parses far enough to look like a group, and the checksum fails.
	for i := len(b) / 4; i < len(b)/2; i++ {
		b[i] ^= 0xFF
	}
	if err := os.WriteFile(path, b, DataFileMode); err != nil {
		t.Fatal(err)
	}
	return dir, badID
}

func groupFileName(id uint64) string {
	return "group-" + itoa64(id) + ".bin"
}

func itoa64(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// The default refuses to open, and says which group. A store that opened
// silently short would answer every query touching that range with missing
// data and nothing anywhere would say so.
func TestCorruptGroupFailsTheOpenByDefault(t *testing.T) {
	dir, badID := corruptStore(t, 5)

	_, err := OpenStore(dir)
	if err == nil {
		t.Fatal("the store opened with a corrupt group under the default policy")
	}
	if !strings.Contains(err.Error(), itoa64(badID)) {
		t.Errorf("error %q does not name the corrupt group %d", err, badID)
	}

	// Nothing was moved: the default policy does not touch the directory.
	if _, serr := os.Stat(filepath.Join(dir, QuarantineDirName)); serr == nil {
		t.Error("the fail policy created a quarantine directory")
	}
	if _, serr := os.Stat(filepath.Join(dir, groupFileName(badID))); serr != nil {
		t.Errorf("the fail policy moved the corrupt group: %v", serr)
	}
}

// Quarantine opens with what is readable, moves the rest aside, and reports
// the store degraded.
func TestQuarantineOpensWithTheHealthyGroups(t *testing.T) {
	const n = 5
	dir, badID := corruptStore(t, n)

	st, err := OpenStoreWith(dir, OpenOptions{Policy: CorruptionQuarantine})
	if err != nil {
		t.Fatalf("quarantine policy did not open the store: %v", err)
	}
	defer st.Close()

	h := st.Health()
	if h.Groups != n-1 {
		t.Errorf("serving %d groups, want %d", h.Groups, n-1)
	}
	if h.Corrupt != 1 {
		t.Errorf("corrupt count %d, want 1", h.Corrupt)
	}
	if !h.Degraded() {
		t.Error("a store with a quarantined group is not reporting degraded")
	}
	if h.Ready() {
		t.Error("a degraded store is READY before anyone acknowledged it")
	}
	if !strings.Contains(h.LastError, itoa64(badID)) {
		t.Errorf("LastError %q does not name group %d", h.LastError, badID)
	}

	// Every healthy batch is readable, and the corrupt one is not there.
	got := storedBatches(t, st)
	present := 0
	for b := 0; b < n; b++ {
		if count(got, b) == 1 {
			present++
		}
	}
	if present != n-1 {
		t.Errorf("%d healthy batches readable, want %d", present, n-1)
	}
}

// The quarantined file moves out of the store and its record says where it
// was, why, and what it checksummed to.
func TestQuarantineRecordsTheEvidence(t *testing.T) {
	dir, badID := corruptStore(t, 3)
	original := filepath.Join(dir, groupFileName(badID))
	originalSum, originalSize, err := checksumFile(original)
	if err != nil {
		t.Fatal(err)
	}

	st, err := OpenStoreWith(dir, OpenOptions{Policy: CorruptionQuarantine})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// Gone from the store, present in quarantine.
	if _, serr := os.Stat(original); serr == nil {
		t.Error("the corrupt group is still in the store directory")
	}
	recs, err := QuarantinedGroups(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("%d quarantine records, want 1", len(recs))
	}
	r := recs[0]
	// The quarantined name carries the checksum as well as the id, because
	// ids are reused: two quarantines of "group 2" with different bodies must
	// not land on one name and destroy the first one's record.
	if r.QuarantinedName == "" {
		t.Error("record does not say what the file was named in quarantine")
	}
	moved := filepath.Join(dir, QuarantineDirName, r.QuarantinedName)
	if _, serr := os.Stat(moved); serr != nil {
		t.Fatalf("the corrupt group is not in quarantine under its recorded name: %v", serr)
	}
	if r.GroupID != badID {
		t.Errorf("record names group %d, want %d", r.GroupID, badID)
	}
	if r.OriginalPath != original {
		t.Errorf("record's original path is %q, want %q", r.OriginalPath, original)
	}
	if r.Reason == "" {
		t.Error("record carries no reason")
	}
	// The checksum is of the bytes as they were, which is what distinguishes
	// "already corrupt on disk" from "changed after the move".
	if r.CRC32C != originalSum {
		t.Errorf("record checksum %d, the file checksummed to %d", r.CRC32C, originalSum)
	}
	if r.Bytes != originalSize {
		t.Errorf("record size %d, want %d", r.Bytes, originalSize)
	}
	if r.QuarantinedAt == "" {
		t.Error("record carries no timestamp")
	}
}

// Acknowledgement is what makes a degraded store ready, and it PERSISTS,
// together with the count it accepted. One more quarantined group makes the
// counts differ and the store is unacknowledged again.
func TestAcknowledgementMakesADegradedStoreReady(t *testing.T) {
	dir, _ := corruptStore(t, 3)

	st, err := OpenStoreWith(dir, OpenOptions{Policy: CorruptionQuarantine})
	if err != nil {
		t.Fatal(err)
	}
	if st.Health().Ready() {
		t.Fatal("degraded and ready before acknowledgement")
	}
	if err := st.AcknowledgeDegraded(); err != nil {
		t.Fatal(err)
	}
	h := st.Health()
	if !h.Ready() {
		t.Error("still not ready after acknowledgement")
	}
	if !h.Degraded() {
		t.Error("acknowledgement cleared the degraded state; it records a decision, not a repair")
	}
	st.Close()

	// A second open finds nothing left to quarantine -- the manifest no longer
	// names the bad group -- but the data is still gone, so it is still
	// degraded, and the acknowledgement PERSISTS so it is still ready.
	//
	// The first version of this feature neither counted the quarantine in
	// Degraded() nor persisted the acknowledgement, so a restart reported a
	// healthy store one restart after permanent loss. The alert metric read
	// zero at exactly the moment it mattered.
	st2, err := OpenStoreWith(dir, OpenOptions{Policy: CorruptionQuarantine})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	h2 := st2.Health()
	if h2.Corrupt != 0 {
		t.Errorf("the second open found %d corrupt groups; the first one removed it from the manifest", h2.Corrupt)
	}
	if !h2.Degraded() {
		t.Error("the store reports healthy after a restart, although a group is still quarantined")
	}
	if h2.Quarantined != 1 {
		t.Errorf("quarantined count %d, want 1: the directory still holds the evidence", h2.Quarantined)
	}
	if !h2.Acknowledged {
		t.Error("the acknowledgement did not survive the restart")
	}
	if !h2.Ready() {
		t.Error("an acknowledged degraded store is not ready after a restart")
	}
}

// After a quarantine the manifest must no longer name the group. Otherwise the
// next open reports "committed but its file is missing" and the store that
// quarantine was supposed to keep serving does not open at all.
func TestQuarantineRemovesTheGroupFromTheManifest(t *testing.T) {
	dir, _ := corruptStore(t, 3)

	st, err := OpenStoreWith(dir, OpenOptions{Policy: CorruptionQuarantine})
	if err != nil {
		t.Fatal(err)
	}
	st.Close()

	// The DEFAULT policy must now open it cleanly: there is no longer a
	// committed group that cannot be read.
	st2, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("after a quarantine the store does not open under the default policy: %v", err)
	}
	defer st2.Close()
	h := st2.Health()
	if h.Corrupt != 0 {
		t.Errorf("the second open still finds %d corrupt groups", h.Corrupt)
	}
	// It opens, and it is still DEGRADED: the group is gone from the manifest
	// and gone from the store, which is exactly the durable loss the health
	// surface exists to keep visible. A degradation that cleared here would
	// read healthy one restart after data loss.
	if !h.Degraded() {
		t.Error("the store reports healthy although a group is still quarantined")
	}
	if h.Quarantined != 1 {
		t.Errorf("quarantined %d, want 1", h.Quarantined)
	}
}

// Several corrupt groups at once, so the count is a count and not a boolean.
func TestQuarantineHandlesSeveralCorruptGroups(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	const n = 6
	var ids []uint64
	for b := 0; b < n; b++ {
		id, err := st.AppendGroup(crashGroup(b))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	st.Close()

	for _, id := range []uint64{ids[1], ids[3], ids[4]} {
		path := filepath.Join(dir, groupFileName(id))
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for i := len(b) / 4; i < len(b)/2; i++ {
			b[i] ^= 0xFF
		}
		if err := os.WriteFile(path, b, DataFileMode); err != nil {
			t.Fatal(err)
		}
	}

	st2, err := OpenStoreWith(dir, OpenOptions{Policy: CorruptionQuarantine})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	h := st2.Health()
	if h.Corrupt != 3 {
		t.Errorf("corrupt count %d, want 3", h.Corrupt)
	}
	if h.Groups != n-3 {
		t.Errorf("serving %d groups, want %d", h.Groups, n-3)
	}
	if h.Quarantined != 3 {
		t.Errorf("quarantined %d, want 3", h.Quarantined)
	}
	recs, err := QuarantinedGroups(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Errorf("%d records, want 3", len(recs))
	}
}

// A healthy store reports healthy and ready, and creates no quarantine
// directory. The negative case matters: a health surface that always says
// degraded is as useless as one that never does.
func TestHealthyStoreIsHealthy(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for b := 0; b < 3; b++ {
		if _, err := st.AppendGroup(crashGroup(b)); err != nil {
			t.Fatal(err)
		}
	}
	h := st.Health()
	if h.Degraded() || !h.Ready() {
		t.Errorf("a healthy store reports %s", h)
	}
	if h.Groups != 3 {
		t.Errorf("Groups = %d, want 3", h.Groups)
	}
	if h.Corrupt != 0 || h.Quarantined != 0 || h.LastError != "" {
		t.Errorf("a healthy store carries corruption state: %+v", h)
	}
	if !strings.HasPrefix(h.String(), "healthy") {
		t.Errorf("String() = %q", h.String())
	}
}

func TestParseCorruptionPolicy(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want CorruptionPolicy
		ok   bool
	}{
		{"", CorruptionFail, true},
		{"fail", CorruptionFail, true},
		{"FAIL", CorruptionFail, true},
		{" quarantine ", CorruptionQuarantine, true},
		{"quarintine", CorruptionFail, false},
		{"skip", CorruptionFail, false},
	} {
		got, err := ParseCorruptionPolicy(tc.in)
		if tc.ok && err != nil {
			t.Errorf("ParseCorruptionPolicy(%q): %v", tc.in, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("ParseCorruptionPolicy(%q) accepted an unknown policy", tc.in)
		}
		if got != tc.want {
			t.Errorf("ParseCorruptionPolicy(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// Health is read on every readiness probe, so it must not allocate.
func TestHealthDoesNotAllocate(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.AppendGroup(crashGroup(0)); err != nil {
		t.Fatal(err)
	}
	var h Health
	if n := testing.AllocsPerRun(50, func() { h = st.Health() }); n != 0 {
		t.Errorf("Health() allocated %.1f times per run", n)
	}
	_ = h
}

// A quarantine that cannot write its record must not move the file. Losing the
// evidence is worse than leaving the group where it is: the record is the only
// thing that says where a quarantined file came from.
func TestQuarantineFailureLeavesTheGroupInPlace(t *testing.T) {
	dir, badID := corruptStore(t, 3)
	// A regular file where the quarantine directory needs to be, so MkdirAll
	// fails.
	if err := os.WriteFile(filepath.Join(dir, QuarantineDirName), []byte("x"), DataFileMode); err != nil {
		t.Fatal(err)
	}

	_, err := OpenStoreWith(dir, OpenOptions{Policy: CorruptionQuarantine})
	if err == nil {
		t.Fatal("the open succeeded although the group could not be quarantined")
	}
	if !strings.Contains(err.Error(), "quarantine") {
		t.Errorf("error %q does not say the quarantine failed", err)
	}
	if _, serr := os.Stat(filepath.Join(dir, groupFileName(badID))); serr != nil {
		t.Errorf("the group was moved although its record could not be written: %v", serr)
	}
	var perr *os.PathError
	_ = errors.As(err, &perr) // the cause is a path error; not asserted, only unwrapped
}

// A legacy directory gets the same policy as a committed store.
//
// The bootstrap loop excluded any group it could not read, silently, so the
// group never reached the loop that applies the policy: `fail` did not fail,
// and Health reported a clean store that was missing data.
func TestLegacyDirectoryAppliesTheCorruptionPolicy(t *testing.T) {
	build := func(t *testing.T) string {
		t.Helper()
		dir, _ := corruptStore(t, 3)
		// Remove the manifest: the directory now looks like one written
		// before the manifest existed.
		if err := os.Remove(filepath.Join(dir, ManifestFileName)); err != nil {
			t.Fatal(err)
		}
		return dir
	}

	t.Run("fail refuses it", func(t *testing.T) {
		dir := build(t)
		st, err := OpenStore(dir)
		if err == nil {
			h := st.Health()
			st.Close()
			t.Fatalf("a legacy directory with a corrupt group opened under the default "+
				"policy and reported %s", h)
		}
	})

	t.Run("quarantine moves it aside", func(t *testing.T) {
		dir := build(t)
		st, err := OpenStoreWith(dir, OpenOptions{Policy: CorruptionQuarantine})
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		h := st.Health()
		if h.Corrupt != 1 {
			t.Errorf("corrupt %d, want 1", h.Corrupt)
		}
		if h.Groups != 2 {
			t.Errorf("serving %d groups, want 2", h.Groups)
		}
		if !h.Degraded() {
			t.Error("a legacy directory that lost a group reports healthy")
		}
	})
}

// A quarantine interrupted between the rename and the manifest commit must be
// RECOVERABLE. It was not: the missing-file check ran before the policy check,
// so every later open -- under either policy -- returned "committed but its
// file is missing" and the store never opened again. Quarantine could not
// recover from its own crash window.
func TestInterruptedQuarantineRecoversOnTheNextOpen(t *testing.T) {
	dir, badID := corruptStore(t, 3)

	// Stop in the window: the file is moved and its record written, and the
	// manifest commit fails.
	injected := errors.New("injected")
	restore := setFaultHook(func(p faultPoint) error {
		if p == faultManifestWrite {
			return injected
		}
		return nil
	})
	_, err := OpenStoreWith(dir, OpenOptions{Policy: CorruptionQuarantine})
	restore()
	if err == nil {
		t.Fatal("the open succeeded although the manifest commit was made to fail")
	}

	// The state the window leaves: file in quarantine, manifest still naming
	// it.
	if _, serr := os.Stat(filepath.Join(dir, groupFileName(badID))); serr == nil {
		t.Fatal("the fixture needs the group moved out of the store")
	}

	// Both policies must recover. The data is already gone; refusing forever
	// helps nobody, and the record in quarantine/ is what distinguishes this
	// from a file someone deleted behind the manifest's back.
	for _, tc := range []struct {
		name string
		opts OpenOptions
	}{
		{"quarantine", OpenOptions{Policy: CorruptionQuarantine}},
		{"fail", OpenOptions{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d2, bad2 := corruptStore(t, 3)
			restore := setFaultHook(func(p faultPoint) error {
				if p == faultManifestWrite {
					return injected
				}
				return nil
			})
			_, _ = OpenStoreWith(d2, OpenOptions{Policy: CorruptionQuarantine})
			restore()

			st, err := OpenStoreWith(d2, tc.opts)
			if err != nil {
				t.Fatalf("the next open did not recover from the crash window: %v", err)
			}
			defer st.Close()
			got := storedBatches(t, st)
			if n := count(got, 1); n != 0 {
				t.Errorf("the quarantined batch is present %d times", n)
			}
			present := 0
			for b := 0; b < 3; b++ {
				if count(got, b) == 1 {
					present++
				}
			}
			if present != 2 {
				t.Errorf("%d healthy batches readable, want 2 (bad id %d)", present, bad2)
			}
		})
	}
}

// A file the manifest names that is NOT in quarantine is still a hard error.
// The recovery above must not become "any missing group is fine".
func TestMissingGroupWithNoQuarantineRecordIsStillFatal(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	id, err := st.AppendGroup(crashGroup(0))
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	if err := os.Remove(filepath.Join(dir, groupFileName(id))); err != nil {
		t.Fatal(err)
	}

	for _, opts := range []OpenOptions{{}, {Policy: CorruptionQuarantine}} {
		if _, err := OpenStoreWith(dir, opts); err == nil {
			t.Errorf("policy %v opened a store whose committed group was deleted", opts.Policy)
		}
	}
}

// The record is written BEFORE the rename, and this is what pins the order.
//
// Reviewer mutation M4 inverted it -- rename first, then write the record --
// and the whole suite stayed green, though the entire design section of
// docs/lld/storage.md rests on that order. A quarantined file with no record
// is evidence destroyed: nothing says which group it was, where it came from,
// or what it checksummed to.
//
// The fault fires at the temp-file create inside writeFileAtomic, which is the
// record write. If the record is written first, the rename has not happened
// and the group is still in the store; if the rename went first, the group is
// in quarantine with no record.
func TestQuarantineWritesTheRecordBeforeMovingTheFile(t *testing.T) {
	dir, badID := corruptStore(t, 3)

	injected := errors.New("injected")
	restore := setFaultHook(func(p faultPoint) error {
		if p == faultCreate {
			return injected
		}
		return nil
	})
	_, err := OpenStoreWith(dir, OpenOptions{Policy: CorruptionQuarantine})
	restore()
	if err == nil {
		t.Fatal("the open succeeded although the record write was made to fail")
	}

	// The group must still be in the store: nothing was moved, because the
	// record could not be written.
	if _, serr := os.Stat(filepath.Join(dir, groupFileName(badID))); serr != nil {
		t.Errorf("the group was MOVED although its record could not be written (%v). "+
			"A quarantined file with no record is evidence destroyed.", serr)
	}
	// And nothing is in quarantine.
	ents, _ := os.ReadDir(filepath.Join(dir, QuarantineDirName))
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".bin") {
			t.Errorf("quarantine holds %s with no record beside it", e.Name())
		}
	}
}

// Two quarantines of the SAME group id must not collide. Ids are reused, and
// the second rename used to land on the first one's name and the second record
// write over the first one's record -- destroying exactly the thing quarantine
// exists to keep.
func TestTwoQuarantinesOfOneIdKeepBothRecords(t *testing.T) {
	dir, badID := corruptStore(t, 3)

	st, err := OpenStoreWith(dir, OpenOptions{Policy: CorruptionQuarantine})
	if err != nil {
		t.Fatal(err)
	}
	// A fresh group takes a NEW id: nextID must have advanced past the
	// quarantined one.
	newID, err := st.AppendGroup(crashGroup(9))
	if err != nil {
		t.Fatal(err)
	}
	if newID == badID {
		t.Fatalf("a new group reused the quarantined id %d", badID)
	}
	st.Close()

	// Corrupt the new one and quarantine it too.
	path := filepath.Join(dir, groupFileName(newID))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := len(b) / 4; i < len(b)/2; i++ {
		b[i] ^= 0xFF
	}
	if err := os.WriteFile(path, b, DataFileMode); err != nil {
		t.Fatal(err)
	}
	st2, err := OpenStoreWith(dir, OpenOptions{Policy: CorruptionQuarantine})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()

	recs, err := QuarantinedGroups(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("%d quarantine records, want 2: one quarantine overwrote another's evidence", len(recs))
	}
	if h := st2.Health(); h.Quarantined != 2 {
		t.Errorf("quarantined count %d, want 2", h.Quarantined)
	}
}

// Health.String names the policy, so an operator reading a readiness body or a
// log line knows which behaviour produced the state. Policy and String() were
// both written and never read, which is how a field goes stale.
func TestHealthStringNamesThePolicy(t *testing.T) {
	dir, _ := corruptStore(t, 3)
	st, err := OpenStoreWith(dir, OpenOptions{Policy: CorruptionQuarantine})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	got := st.Health().String()
	if !strings.Contains(got, "quarantine") {
		t.Errorf("Health.String() = %q, does not name the policy", got)
	}
	if CorruptionFail.String() != "fail" || CorruptionQuarantine.String() != "quarantine" {
		t.Errorf("policy names are %q and %q", CorruptionFail, CorruptionQuarantine)
	}
	if got := CorruptionPolicy(99).String(); !strings.Contains(got, "99") {
		t.Errorf("an unknown policy prints %q", got)
	}
}

// A quarantine directory that cannot be read is not a healthy store. Returning
// 0 for it -- which ignoring the ReadDir error does -- prints a clean health
// surface over a permissions or IO problem sitting on top of the evidence.
func TestUnreadableQuarantineDirectoryIsNotHealthy(t *testing.T) {
	dir, _ := corruptStore(t, 3)
	st, err := OpenStoreWith(dir, OpenOptions{Policy: CorruptionQuarantine})
	if err != nil {
		t.Fatal(err)
	}
	st.Close()

	qdir := filepath.Join(dir, QuarantineDirName)
	if err := os.Chmod(qdir, 0o000); err != nil {
		t.Skipf("cannot remove read permission: %v", err)
	}
	defer os.Chmod(qdir, DirFileMode)
	if os.Geteuid() == 0 {
		t.Skip("running as root; a mode of 000 does not stop a read")
	}

	st2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	h := st2.Health()
	if !h.Degraded() {
		t.Errorf("a store whose quarantine directory will not open reports %s", h)
	}
	if h.LastError == "" {
		t.Error("nothing says why")
	}
}

// The quarantine path fsyncs BOTH directory entries the rename touched.
//
// This was recorded as uncatchable — "proving a directory sync needs the
// unsynced entries actually dropped, which is a power-loss rig this repository
// does not have". That is true of proving the KERNEL dropped something and
// false of what the mutation actually removes: syncDir already calls
// fault(faultDirSync), the same injection point the crash matrix uses. The
// calls can be counted, and an error injected into one must reach the caller.
//
// It does not prove durability. It proves the syncs happen, which is exactly
// what deleting them stops.
func TestQuarantineSyncsBothDirectories(t *testing.T) {
	dir, _ := corruptStore(t, 3)

	var dirSyncs int
	restore := setFaultHook(func(p faultPoint) error {
		if p == faultDirSync {
			dirSyncs++
		}
		return nil
	})
	st, err := OpenStoreWith(dir, OpenOptions{Policy: CorruptionQuarantine})
	restore()
	if err != nil {
		t.Fatal(err)
	}
	st.Close()

	// The record's writeFileAtomic syncs one, the MkdirAll syncs the store
	// directory, and the rename syncs both ends: four. Asserted as a lower
	// bound on the two the rename needs, so an added durable write elsewhere
	// in the path does not make this fail for the wrong reason.
	if dirSyncs < 3 {
		t.Errorf("the quarantine path fsynced %d directories; the rename alone needs both ends",
			dirSyncs)
	}

	// And an error from one of them must reach the caller rather than being
	// dropped: a sync whose failure is ignored is a sync that is not there.
	dir2, _ := corruptStore(t, 3)
	injected := errors.New("injected")
	n := 0
	restore2 := setFaultHook(func(p faultPoint) error {
		if p == faultDirSync {
			n++
			if n >= 3 {
				return injected
			}
		}
		return nil
	})
	_, err = OpenStoreWith(dir2, OpenOptions{Policy: CorruptionQuarantine})
	restore2()
	if err == nil {
		t.Error("a failing directory sync in the quarantine path was swallowed")
	}
}

// A quarantine record that does not match its group must not authorize
// dropping that group from the manifest.
//
// The gate was a filename check: one empty `group-1-00000000.bin.json` made a
// genuinely missing group open as "quarantined by an earlier open", under the
// DEFAULT policy, with nothing quarantined and no record listed. A store that
// says a group was quarantined and has nothing quarantined has laundered a
// missing group into a clean state.
func TestStrayQuarantineRecordDoesNotAuthorizeDroppingAGroup(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(t *testing.T, qdir string, id uint64)
	}{
		{"an empty record", func(t *testing.T, qdir string, id uint64) {
			p := filepath.Join(qdir, "group-"+itoa64(id)+"-00000000.bin.json")
			if err := os.WriteFile(p, nil, DataFileMode); err != nil {
				t.Fatal(err)
			}
		}},
		{"a record naming a different group", func(t *testing.T, qdir string, id uint64) {
			p := filepath.Join(qdir, "group-"+itoa64(id)+"-00000000.bin.json")
			body := []byte(`{"group_id":999,"quarantined_name":"group-999-0.bin"}`)
			if err := os.WriteFile(p, body, DataFileMode); err != nil {
				t.Fatal(err)
			}
		}},
		{"a record whose file is absent", func(t *testing.T, qdir string, id uint64) {
			p := filepath.Join(qdir, "group-"+itoa64(id)+"-00000000.bin.json")
			body := []byte(`{"group_id":` + itoa64(id) + `,"quarantined_name":"group-` +
				itoa64(id) + `-00000000.bin"}`)
			if err := os.WriteFile(p, body, DataFileMode); err != nil {
				t.Fatal(err)
			}
		}},
		{"not JSON at all", func(t *testing.T, qdir string, id uint64) {
			p := filepath.Join(qdir, "group-"+itoa64(id)+"-00000000.bin.json")
			if err := os.WriteFile(p, []byte("not json"), DataFileMode); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			st, err := OpenStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			var ids []uint64
			for b := 0; b < 3; b++ {
				id, err := st.AppendGroup(crashGroup(b))
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
			}
			st.Close()

			victim := ids[1]
			qdir := filepath.Join(dir, QuarantineDirName)
			if err := os.MkdirAll(qdir, DirFileMode); err != nil {
				t.Fatal(err)
			}
			tc.write(t, qdir, victim)
			// The group really is gone from the store.
			if err := os.Remove(filepath.Join(dir, groupFileName(victim))); err != nil {
				t.Fatal(err)
			}

			for _, opts := range []OpenOptions{{}, {Policy: CorruptionQuarantine}} {
				st2, err := OpenStoreWith(dir, opts)
				if err == nil {
					h := st2.Health()
					st2.Close()
					t.Errorf("policy %v opened a store whose committed group is missing, "+
						"on the strength of %s: %s", opts.Policy, tc.name, h)
				}
			}
		})
	}
}

// A quarantined id is never reissued, across opens.
//
// nextID advanced past every id in the manifest -- and the quarantining open
// REMOVES the id from the manifest, so the next open regressed past it. The
// store then handed that id to real data, and when that file went missing the
// stale record made a genuine loss read as an old quarantine.
func TestQuarantinedIDIsNeverReissued(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	var ids []uint64
	for b := 0; b < 3; b++ {
		id, err := st.AppendGroup(crashGroup(b))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	st.Close()

	// Corrupt the HIGHEST id, so quarantining it is what moves nextID.
	top := ids[len(ids)-1]
	path := filepath.Join(dir, groupFileName(top))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := len(b) / 4; i < len(b)/2; i++ {
		b[i] ^= 0xFF
	}
	if err := os.WriteFile(path, b, DataFileMode); err != nil {
		t.Fatal(err)
	}

	st2, err := OpenStoreWith(dir, OpenOptions{Policy: CorruptionQuarantine})
	if err != nil {
		t.Fatal(err)
	}
	st2.Close()

	// A FRESH open -- the one whose visibleIDs no longer holds the quarantined
	// id -- must still not reissue it.
	st3, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st3.Close()
	newID, err := st3.AppendGroup(crashGroup(9))
	if err != nil {
		t.Fatal(err)
	}
	if newID <= top {
		t.Fatalf("a new group took id %d, at or below the quarantined id %d: "+
			"the id was reissued across opens", newID, top)
	}
}
