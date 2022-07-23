package raft

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func RandIntRange(min int, max int) int {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	return r1.Intn(max-min) + int(min)
}

func MakeAnRandomElectionTimeout(base int) int {
	return RandIntRange(base, 2*base)
}

func PrintDebugLog(msg string) {
	fmt.Printf("%s %s \n", time.Now().Format("2006-01-02 15:04:05"), msg)
}

func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}
