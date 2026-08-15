package storage

import (
	"flag"
	"fmt"
	"sort"
	"strings"
)

// The fault injector, exported for the packages above this one.
//
// A storage-package test can already fail any step of the durable write --
// setFaultHook has been here since the atomic-replacement work. What it cannot
// do is answer the question that matters to an operator: when the disk fills
// during a write, what does the CLIENT see? That answer is assembled three
// layers up, out of the writer's batch accounting and the handler's status
// code, and testing it needs the fault to fire while a real request is in
// flight through a real writer.
//
// So the hook is exported, and guarded rather than build-tagged. A build tag
// would put the ingest failure suite in a lane of its own, and a lane that is
// not part of `make verify` is a lane that goes stale -- this repository has
// already paid for one vacuously-green tagged lane. The guard below costs one
// map lookup at arm time and makes production misuse impossible instead of
// merely discouraged.

// FaultPoint names one step of the durable write path. Values are opaque;
// obtain them from FaultPointNamed.
type FaultPoint = faultPoint

// FaultPointNamed resolves a fault point by the name the crash matrix uses on
// its command line. FaultPointNames() is the list; this comment deliberately
// does not repeat it, because the paragraph below explains that a hand-written
// list is what goes stale -- and the one that used to be here had already
// fallen three names behind.
func FaultPointNamed(name string) (FaultPoint, bool) {
	for p, n := range faultPointName {
		if n == name {
			return p, true
		}
	}
	return 0, false
}

// FaultPointNames lists every name FaultPointNamed accepts, sorted. A test
// that sweeps the whole write path enumerates it rather than repeating a list
// that a new fault point would silently fall out of.
func FaultPointNames() []string {
	out := make([]string, 0, len(faultPointName))
	for _, n := range faultPointName {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// NonWriteFaultPointNames lists the fault points that are NOT steps of the
// durable write, so a sweep over FaultPointNames can exclude them without
// hardcoding a list of its own.
//
// Inverted deliberately. A sweep that enumerated a WRITE-path list would
// silently miss the next write step someone forgets to add to it, which is the
// exact failure the sweep exists to prevent. Excluding a small closed set that
// only grows when a non-write point is deliberately added keeps every new
// write step covered the day it exists.
func NonWriteFaultPointNames() []string {
	return []string{
		// Not steps of the write: they exist so the crash matrix can stop
		// with rows buffered and after an acknowledgement.
		faultPointName[faultBuffered],
		faultPointName[faultPostAck],
		// A restore is not an ingest. This one fires after a staged restore
		// has renamed its staging directory into place.
		faultPointName[faultRestoreRenamed],
		faultPointName[faultRestoreRemoved],
		faultPointName[faultRestoreReleasing],
		// Not a step of the write either: it is a step of taking the lock,
		// and a sweep that failed it would be testing that lockDir propagates
		// an error rather than anything about durability.
		faultPointName[faultLockOpened],
		// Not a step of the write either: it is a step of a restore's cleanup.
		faultPointName[faultRestoreCleanup],
	}
}

// SetFaultHookForTest installs a fault injector into the durable write path
// and returns a function restoring the previous one. The hook is called at
// every step named by FaultPointNames; returning a non-nil error makes that
// step fail exactly as the real syscall would.
//
// It panics outside a test binary. The check is the -test.v flag the testing
// package registers in TestMain, which exists in every test binary and in no
// production one; testing itself is deliberately not imported, since pulling
// it into the production import graph to guard against production use is a
// trade in the wrong direction.
func SetFaultHookForTest(h func(FaultPoint) error) func() {
	if flag.Lookup("test.v") == nil {
		panic("storage: SetFaultHookForTest called outside a test binary")
	}
	return setFaultHook(h)
}

// FaultPointString names a point for test output.
func FaultPointString(p FaultPoint) string {
	if n, ok := faultPointName[p]; ok {
		return n
	}
	return fmt.Sprintf("fault(%d)", int(p))
}

// FailAt builds a hook failing exactly the named point with err. Names that
// do not resolve are reported rather than ignored, because a typo in a fault
// name produces a test that injects nothing and passes.
func FailAt(name string, err error) (func(FaultPoint) error, error) {
	target, ok := FaultPointNamed(name)
	if !ok {
		return nil, fmt.Errorf("storage: no fault point named %q; have %s",
			name, strings.Join(FaultPointNames(), ", "))
	}
	return func(p FaultPoint) error {
		if p == target {
			return err
		}
		return nil
	}, nil
}
