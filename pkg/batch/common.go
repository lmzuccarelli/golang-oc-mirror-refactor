package batch

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"time"

	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
)

const (
	logFile                 string = "worker-{batch}.log"
	workerPrefix            string = "[Worker] "
	ConcurrentWorker        string = "ConcurrentWorker"
	ChannelConcurrentWorker string = "ChannelConcurrentWorker"
)

func saveErrors(logger clog.PluggableLoggerInterface, logsDir string, errArray []mirrorSchemaError) (string, error) {
	if len(errArray) > 0 {
		timestamp := time.Now().Format("20060102_150405")
		filename := fmt.Sprintf("mirroring_errors_%s.txt", timestamp)
		file, err := os.Create(filepath.Join(logsDir, filename))
		if err != nil {
			logger.Error(workerPrefix+"failed to create file: %s", err.Error())
			return filename, fmt.Errorf("%w", err)
		}
		defer file.Close()

		for _, err := range errArray {
			errorMsg := formatErrorMsg(err)
			logger.Error(workerPrefix + errorMsg)
			fmt.Fprintln(file, errorMsg)
		}
		return filename, nil
	}
	return "", nil
}

func formatErrorMsg(err mirrorSchemaError) string {
	if len(err.operators) > 0 || len(err.bundles) > 0 {
		return fmt.Sprintf("error mirroring image %s (Operator bundles: %v - Operators: %v) error: %s", err.image.Origin, maps.Values(err.bundles), maps.Keys(err.operators), err.err.Error())
	}

	return fmt.Sprintf("error mirroring image %s error: %s", err.image.Origin, err.err.Error())
}

func (s StringMap) Has(key string) bool {
	_, ok := s[key]
	return ok
}
