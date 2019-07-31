package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/hashicorp/logutils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logFilter = logutils.LevelFilter{
	Levels:   []logutils.LogLevel{"DEBUG", "INFO", "WARN", "ERROR"},
	MinLevel: logutils.LogLevel("DEBUG"),
	Writer:   os.Stderr,
}

func configureLogger(logLevel string) *zap.Logger {
	var level zap.AtomicLevel
	var syncOutput zapcore.WriteSyncer
	switch strings.ToLower(logLevel) {
	case "none":
		return zap.NewNop()
	case "", "info":
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
		logFilter.MinLevel = logutils.LogLevel("INFO")
	case "debug":
		level = zap.NewAtomicLevelAt(zap.DebugLevel)
		logFilter.MinLevel = logutils.LogLevel("DEBUG")
	case "warn":
		level = zap.NewAtomicLevelAt(zap.WarnLevel)
		logFilter.MinLevel = logutils.LogLevel("WARN")
	case "error":
		level = zap.NewAtomicLevelAt(zap.ErrorLevel)
		logFilter.MinLevel = logutils.LogLevel("ERROR")
	case "panic":
		level = zap.NewAtomicLevelAt(zap.PanicLevel)
		logFilter.MinLevel = logutils.LogLevel("ERROR")
	case "fatal":
		level = zap.NewAtomicLevelAt(zap.FatalLevel)
		logFilter.MinLevel = logutils.LogLevel("ERROR")
	default:
		fmt.Printf("Invalid log level supplied. Defaulting to info: %s", logLevel)
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}
	syncOutput = zapcore.Lock(os.Stdout)
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderCfg),
		syncOutput,
		level,
	)
	logger := zap.New(core)
	//zap.ReplaceGlobals(logger)
	return logger
}
