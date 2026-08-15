//go:build windows

package storage

func setUmask(int) int { return 0 }

const umaskSupported = false
