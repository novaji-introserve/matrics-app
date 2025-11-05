package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// LogConfig holds logging configuration
type LogConfig struct {
	Level      string
	Format     string
	Output     string
	File       string
	MaxSize    int
	MaxBackups int
	MaxAge     int
}

// NewLogger creates a new configured logger
func NewLogger(cfg LogConfig) (*zap.Logger, error) {
	// Parse log level
	var level zapcore.Level
	switch strings.ToUpper(cfg.Level) {
	case "DEBUG":
		level = zapcore.DebugLevel
	case "INFO":
		level = zapcore.InfoLevel
	case "WARN":
		level = zapcore.WarnLevel
	case "ERROR":
		level = zapcore.ErrorLevel
	default:
		level = zapcore.InfoLevel
	}

	// Configure encoder
	var encoder zapcore.Encoder
	
	if strings.ToLower(cfg.Format) == "json" {
		// JSON encoder for machine processing
		encoderCfg := zap.NewProductionEncoderConfig()
		encoderCfg.TimeKey = "timestamp"
		encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
		encoder = zapcore.NewJSONEncoder(encoderCfg)
	} else {
		// Human-readable console encoder with custom formatting
		encoderCfg := zapcore.EncoderConfig{
			TimeKey:        "T",
			LevelKey:       "L",
			NameKey:        "N",
			CallerKey:      "C",            // Include caller information
			FunctionKey:    zapcore.OmitKey,
			MessageKey:     "M",
			StacktraceKey:  "S",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    CustomLevelEncoder,  // Custom colored level
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		}
		encoder = zapcore.NewConsoleEncoder(encoderCfg)
	}

	// Configure output(s)
	var cores []zapcore.Core

	// Add stdout if requested
	if cfg.Output == "stdout" || cfg.Output == "both" {
		stdoutSyncer := zapcore.AddSync(os.Stdout)
		cores = append(cores, zapcore.NewCore(encoder, stdoutSyncer, level))
	}

	// Add file if requested
	if cfg.Output == "file" || cfg.Output == "both" {
		if cfg.File != "" {
			// Ensure log directory exists
			logDir := filepath.Dir(cfg.File)
			if err := os.MkdirAll(logDir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create log directory: %w", err)
			}

			// Configure log rotation
			fileWriter := &lumberjack.Logger{
				Filename:   cfg.File,
				MaxSize:    cfg.MaxSize,    // MB
				MaxBackups: cfg.MaxBackups,
				MaxAge:     cfg.MaxAge,     // days
				Compress:   true,
			}
			
			fileSyncer := zapcore.AddSync(fileWriter)

			// Use a plain text encoder without colors for file output
			fileEncoderCfg := zapcore.EncoderConfig{
				TimeKey:        "T",
				LevelKey:       "L",
				NameKey:        "N",
				CallerKey:      "C",          // Include caller in file logs too
				FunctionKey:    zapcore.OmitKey,
				MessageKey:     "M",
				StacktraceKey:  "S",
				LineEnding:     zapcore.DefaultLineEnding,
				EncodeLevel:    zapcore.CapitalLevelEncoder, // No colors in file
				EncodeTime:     zapcore.ISO8601TimeEncoder,
				EncodeDuration: zapcore.StringDurationEncoder,
				EncodeCaller:   zapcore.ShortCallerEncoder,
			}
			fileEncoder := zapcore.NewConsoleEncoder(fileEncoderCfg)
			
			cores = append(cores, zapcore.NewCore(fileEncoder, fileSyncer, level))
		}
	}

	// Create logger
	core := zapcore.NewTee(cores...)
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	return logger, nil
}

// CustomLevelEncoder adds color to the log level and ensures it's properly spaced
func CustomLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	switch l {
	case zapcore.DebugLevel:
		enc.AppendString("\033[36mDEBUG\033[0m") // Cyan
	case zapcore.InfoLevel:
		enc.AppendString("\033[32mINFO\033[0m")  // Green
	case zapcore.WarnLevel:
		enc.AppendString("\033[33mWARN\033[0m")  // Yellow
	case zapcore.ErrorLevel:
		enc.AppendString("\033[31mERROR\033[0m") // Red
	case zapcore.DPanicLevel:
		enc.AppendString("\033[31mDPANIC\033[0m") // Red
	case zapcore.PanicLevel:
		enc.AppendString("\033[31mPANIC\033[0m") // Red
	case zapcore.FatalLevel:
		enc.AppendString("\033[35mFATAL\033[0m") // Magenta
	default:
		enc.AppendString(l.String())
	}
}

