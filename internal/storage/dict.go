package storage

// The dictionary is stored for random access straight from the mmap'd file:
// D+1 uint32 offsets, then the concatenated sorted strings. A membership
// probe binary-searches it in place -- log(D) page touches, no decode, no
// heap -- so a store of billions of rows answers a needle without
// materializing any group's dictionary in RAM. This replaces the old
// LZ4'd-in-the-footer dict, which had to be decompressed into a []string at
// open time (tens of GB of strings at scale). Footprint trades for it: the
// section is uncompressed, but it is on disk and paged on demand.

// marshalDictSection lays out a sorted dict as [D+1 offsets][strings].
func marshalDictSection(dict []string) []byte {
	strsLen := 0
	for _, s := range dict {
		strsLen += len(s)
	}
	out := make([]byte, 0, 4*(len(dict)+1)+strsLen)
	var off uint32
	for _, s := range dict {
		out = appU32(out, off)
		off += uint32(len(s))
	}
	out = appU32(out, off)
	for _, s := range dict {
		out = append(out, s...)
	}
	return out
}

// dictSectionAt returns the i-th value, copied out of the mapping so it is
// safe to keep after the mapping is released.
func dictSectionAt(sec []byte, n, i int) string {
	base := 4 * (n + 1)
	o0 := get32(sec, 4*i)
	o1 := get32(sec, 4*(i+1))
	return string(sec[base+int(o0) : base+int(o1)])
}

// dictSectionSearch binary-searches the sorted section for value, returning
// its id or -1. The string(sec[...]) comparisons do not allocate -- the
// compiler elides the conversion in a comparison.
func dictSectionSearch(sec []byte, n int, value string) int {
	base := 4 * (n + 1)
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		o0 := get32(sec, 4*mid)
		o1 := get32(sec, 4*(mid+1))
		if string(sec[base+int(o0):base+int(o1)]) < value {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < n {
		o0 := get32(sec, 4*lo)
		o1 := get32(sec, 4*(lo+1))
		if string(sec[base+int(o0):base+int(o1)]) == value {
			return lo
		}
	}
	return -1
}

// dictSectionAll materializes the whole dict -- the scan path (substring,
// regexp, group-by), which needs every value; allocated on demand per query,
// not held at rest.
func dictSectionAll(sec []byte, n int) []string {
	out := make([]string, n)
	base := 4 * (n + 1)
	for i := 0; i < n; i++ {
		o0 := get32(sec, 4*i)
		o1 := get32(sec, 4*(i+1))
		out[i] = string(sec[base+int(o0) : base+int(o1)])
	}
	return out
}
