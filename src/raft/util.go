package raft

import "log"

// Debugging
const Debug = false
const IM = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Im(format string, a ...interface{}) {
	if IM {
		log.Printf(format, a...)
	}
	return
}

func LOG(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func ASSERT(expect bool) {
	if !expect {
		panic("assert失败.....")
	}
}

func max(a int, b int) int {
	if a < b {
		return b
	}
	return a
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
