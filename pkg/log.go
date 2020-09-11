package mk

import (
	"bytes"
	"fmt"
	stdlog "log"
	"os"
	"sort"
	"strings"

	"github.com/go-logr/logr"
)

const (
	logInfo  = 0
	logDebug = 1
)

var (
	log logr.Logger
)

func init() {
	log = newLogger(logDebug)
}

// Log verbosity represents how little a log matters.  Level zero, the default,
// matters most.  Increasing levels matter less and less.  Try to avoid lots of
// different verbosity levels, and instead provide useful keys, logger names,
// and log messages for users to filter on.  It's illegal to pass a log level
// below zero.
var verbosity = logDebug

// Logger implements github.com/go-logr/logr
// Some logic borrowed from github.com/go-logr/stdr
type Logger struct {
	level int
	names []string

	// Variable information can then be attached using named values (key/value
	// pairs).  Keys are arbitrary strings, while values may be any Go value.
	//
	// Keys are not strictly required to conform to any specification or regex, but
	// it is recommended that they:
	//   * be human-readable and meaningful (not auto-generated or simple ordinals)
	//   * be constant (not dependent on input data)
	//   * contain only printable characters
	//   * not contain whitespace or punctuation
	values []interface{}
	stdl   *stdlog.Logger
}

func newLogger(verbosity int) Logger {
	return Logger{
		level:  verbosity,
		names:  []string{},
		values: []interface{}{},
		stdl:   stdlog.New(os.Stdout, "", stdlog.LstdFlags|stdlog.Lshortfile),
	}
}

func (l Logger) copy() Logger {
	new := Logger{
		level:  l.level,
		names:  make([]string, len(l.names)),
		values: make([]interface{}, len(l.values)),
	}
	copy(new.names, l.names)
	copy(new.values, l.values)

	return new
}

// Enabled tests whether this Logger is enabled.
func (l Logger) Enabled() bool {
	return l.level <= verbosity
}

// flatKv generates string representation of keysAndValues
func flatKv(keysAndValues ...interface{}) string {
	var keys []string
	var values []string

	for i := 0; i < len(keysAndValues); i += 2 {
		k, ok := keysAndValues[i].(string)

		// When key can't convert to string, skip this pair
		if !ok {
			continue
		}

		keys = append(keys, k)
		if i+1 >= len(keysAndValues) {
			values = append(values, "")
		}
		v, ok := keysAndValues[i+1].(string)
		if ok {
			values = append(values, v)
		} else {
			values = append(values, "")
		}
	}

	sort.Strings(keys)

	var buf bytes.Buffer
	for i, k := range keys {
		if i > 0 {
			buf.WriteString(" ")
		}
		s := fmt.Sprintf("%s=%s", k, values[i])
		buf.WriteString(s)
	}

	return buf.String()
}

// Info logs a non-error message with the given key/value pairs as context.
func (l Logger) Info(msg string, keysAndValues ...interface{}) {
	if l.Enabled() {
		prefix := strings.Join(l.names, ".")
		lvStr := flatKv("level", l.level)

		// Msg should be a simple description of what's occurring, and should
		// never be a format string.
		msgStr := flatKv("msg", msg)

		annoStr := flatKv(l.values...)
		kvStr := flatKv(keysAndValues...)
		s := fmt.Sprintln(prefix, lvStr, msgStr, annoStr, kvStr)
		l.output(s)
	}
}

func (l Logger) output(s string) {
	l.stdl.Print(s)
}

// Error logs an error, with the given message and key/value pairs as context.
// According to https://dave.cheney.net/2015/11/05/lets-talk-about-logging, every error
// should be returned up to main, and handled there.
func (l Logger) Error(err error, msg string, keysAndValues ...interface{}) {
	prefix := strings.Join(l.names, ".")
	errStr := flatKv("error", err.Error())
	msgStr := flatKv("msg", msg)
	annoStr := flatKv(l.values...)
	kvStr := flatKv(keysAndValues...)
	s := fmt.Sprintln(prefix, errStr, msgStr, annoStr, kvStr)
	l.output(s)
}

// V higher verbosity level means a log message is less important.
func (l Logger) V(level int) logr.Logger {
	if level < 0 {
		panic("Invalid log level")
	}
	new := l.copy()
	new.level = level

	return new
}

// WithName adds a new element to the logger's name.
func (l Logger) WithName(name string) logr.Logger {
	new := l.copy()
	new.names = append(new.names, name)

	return new
}

// WithValues adds some key-value pairs of context to a logger.
func (l Logger) WithValues(keysAndValues ...interface{}) logr.Logger {
	new := l.copy()
	for i := 0; i < len(keysAndValues); i += 2 {
		new.values = append(new.values, keysAndValues[i])
		if i+1 < len(keysAndValues) {
			new.values = append(new.values, keysAndValues[i+1])
		} else {
			new.values = append(new.values, "")
		}
	}

	return new
}
