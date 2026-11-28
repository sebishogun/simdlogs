//go:build linux

package storage

import (
	"bytes"
	"os"
	"testing"
	"unsafe"
)

// mappedPages counts the pages of b present in this process's page tables.
func mappedPages(t *testing.T, b []byte) int {
	t.Helper()
	pm, err := os.Open("/proc/self/pagemap")
	if err != nil {
		t.Skip("no pagemap:", err)
	}
	defer pm.Close()
	base := uintptr(unsafe.Pointer(&b[0]))
	entry := make([]byte, 8)
	n := 0
	for off := 0; off < len(b); off += os.Getpagesize() {
		if _, err := pm.ReadAt(entry, int64((base+uintptr(off))/uintptr(os.Getpagesize())*8)); err != nil {
			t.Fatal(err)
		}
		if entry[7]&0x80 != 0 { // Bit 63: present.
			n++
		}
	}
	return n
}

// A verified group's pages leave the process: the checksum read every byte, and before readMapped
// every group a store wrote or opened stayed resident, so its RSS grew to the store's size. The
// mapping still reads the same bytes.
func TestVerifiedGroupLeavesNoPagesMapped(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	g, _ := buildGroup(100_000)
	want := g.Marshal()
	if len(want) < 16*os.Getpagesize() {
		t.Fatalf("the group is %d bytes, too small to tell", len(want))
	}
	if _, err := s.AppendGroup(g); err != nil {
		t.Fatal(err)
	}
	check := func(when string, s *Store) {
		t.Helper()
		blob := s.groups[0].reader.blob
		if n := mappedPages(t, blob); n > 0 {
			t.Fatalf("%s: %d of the group's %d pages are mapped", when, n, (len(blob)+os.Getpagesize()-1)/os.Getpagesize())
		}
		if !bytes.Equal(blob, want) { // Reads every page back.
			t.Fatalf("%s: the mapping reads different bytes", when)
		}
		if n := mappedPages(t, blob); n == 0 {
			t.Fatalf("%s: reading the group mapped no pages, so the check above proves nothing", when)
		}
	}
	check("after the append", s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err = OpenStore(dir); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	check("after an open", s)
}
