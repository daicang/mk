package mk

import "fmt"

var (
	debugFlag = true
)

func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

func debug(msg string, v ...interface{}) {
	if debugFlag {
		fmt.Printf(msg, v...)
	}
}
