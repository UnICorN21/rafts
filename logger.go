package rafts

import (
	"log"
	"os"
	"fmt"
)

// Logger is an interface for customizing the log utility.
type Logger interface {
	Debug(args ...interface{})
	Debugf(s string, args ...interface{})

	Info(args ...interface{})
	Infof(s string, args ...interface{})

	Warning(args ...interface{})
	Warningf(s string, args ...interface{})

	Error(args ...interface{})
	Errorf(s string, args ...interface{})

	Fatal(args ...interface{})
	Fatalf(s string, args ...interface{})

	Panic(args ...interface{})
	Panicf(s string, args ...interface{})
}

var defaultLogger Logger = &DefaultLogger{log.New(os.Stderr, "raft", log.LstdFlags), true}

const callDepth = 2

type DefaultLogger struct {
	*log.Logger
	debug bool
}

func (l *DefaultLogger) EnableTimestamps() {
	l.SetFlags(l.Flags() | log.Ldate | log.Ltime)
}

func (l *DefaultLogger) EnableDebug() {
	l.debug = true
}

func (l *DefaultLogger) Debug(v ...interface{}) {
	if l.debug {
		l.Output(callDepth, header("DEBUG", fmt.Sprint(v...)))
	}
}

func (l *DefaultLogger) Debugf(format string, v ...interface{}) {
	if l.debug {
		l.Output(callDepth, header("DEBUG", fmt.Sprintf(format, v...)))
	}
}

func (l *DefaultLogger) Info(v ...interface{}) {
	l.Output(callDepth, header("INFO", fmt.Sprint(v...)))
}

func (l *DefaultLogger) Infof(format string, v ...interface{}) {
	l.Output(callDepth, header("INFO", fmt.Sprintf(format, v...)))
}

func (l *DefaultLogger) Error(v ...interface{}) {
	l.Output(callDepth, header("ERROR", fmt.Sprint(v...)))
}

func (l *DefaultLogger) Errorf(format string, v ...interface{}) {
	l.Output(callDepth, header("ERROR", fmt.Sprintf(format, v...)))
}

func (l *DefaultLogger) Warning(v ...interface{}) {
	l.Output(callDepth, header("WARN", fmt.Sprint(v...)))
}

func (l *DefaultLogger) Warningf(format string, v ...interface{}) {
	l.Output(callDepth, header("WARN", fmt.Sprintf(format, v...)))
}

func (l *DefaultLogger) Fatal(v ...interface{}) {
	l.Output(callDepth, header("FATAL", fmt.Sprint(v...)))
	os.Exit(1)
}

func (l *DefaultLogger) Fatalf(format string, v ...interface{}) {
	l.Output(callDepth, header("FATAL", fmt.Sprintf(format, v...)))
	os.Exit(1)
}

func (l *DefaultLogger) Panic(v ...interface{}) {
	l.Logger.Panic(v)
}

func (l *DefaultLogger) Panicf(format string, v ...interface{}) {
	l.Logger.Panicf(format, v...)
}

func header(lvl, msg string) string {
	return fmt.Sprintf("%s: %s", lvl, msg)
}