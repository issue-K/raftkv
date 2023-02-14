package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
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
