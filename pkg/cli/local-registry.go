package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"text/template"
	"time"

	"github.com/distribution/distribution/v3/configuration"
	"github.com/distribution/distribution/v3/registry"
	_ "github.com/distribution/distribution/v3/registry/storage/driver/filesystem"
	//"github.com/google/uuid"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
	"github.com/sirupsen/logrus"
)

type LocalStorageInterface interface {
	Setup(args []string) error
}

type LocalStorage struct {
	Log     clog.PluggableLoggerInterface
	Options *common.MirrorOptions
	Context context.Context
}

// setupLocalStorage - private function that sets up
// a local (distribution) registry
func (o LocalStorage) Setup() error {
	config, err := setupLocalRegistryConfig(o.Options)
	if err != nil {
		o.Log.Error("parsing config %v", err)
	}
	regLogger := logrus.New()
	// prepare the logger
	registryLogPath := filepath.Join(o.Options.LogsDir, registryLogFilename)
	registryLogFile, err := os.Create(registryLogPath)
	if err != nil {
		regLogger.Warn("Failed to create log file for local storage registry, using default stderr")
	} else {
		regLogger.Out = registryLogFile
	}
	absPath, err := filepath.Abs(registryLogPath)

	o.Log.Debug("local storage registry will log to %s", absPath)
	logrus.SetOutput(registryLogFile)
	os.Setenv("OTEL_TRACES_EXPORTER", "none")

	ctx := context.Background()
	reg, err := registry.NewRegistry(ctx, config)
	if err != nil {
		return err
	}
	o.Options.LocalStorageService = *reg
	return nil
}

func (o LocalStorage) StartLocalRegistry() error {
	err := o.Options.LocalStorageService.ListenAndServe()
	if !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("Could not start local registry: %v", err)
	}
	return nil
}

// stopLocalRegistry - stops the local registry and closes the registry.log file
func (o LocalStorage) StopLocalRegistry() {
	// Try to gracefully shutdown the local registry
	ctx, cancel := context.WithTimeout(o.Context, time.Second)
	defer cancel()
	if err := o.Options.LocalStorageService.Shutdown(ctx); err != nil {
		o.Log.Warn("Registry shutdown failure: %v", err)
	}

	if o.Options.RegistryLogFile != nil {
		// NOTE: we cannot just close the registry.log file as it is set as logrus output, which could still be in use
		// by other dependencies before we exit. First we need to make sure logrus uses a different output.
		logrus.SetOutput(io.Discard)
		if err := o.Options.RegistryLogFile.Close(); err != nil {
			o.Log.Warn("Close registry.log failed: %v", err)
		}
	}
}

// isLocalStoragePortBound - private utility to check if port is bound
func isLocalStoragePortBound(opts common.MirrorOptions) bool {

	// Check if the port is already bound
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", opts.Port))
	if err != nil {
		return true
	}
	listener.Close()
	return false
}

// setupLocalRegistryConfig - private function to parse registry config
// used in both localregistry serve and localregistry garbage-collect (for delete)
func setupLocalRegistryConfig(opts *common.MirrorOptions) (*configuration.Configuration, error) {
	// create config file for local registry
	// sonarqube scanner variable declaration convention
	configYamlV01 := `
version: 0.1
log:
  accesslog:
    disabled: {{ .LogAccessOff }}
  level: {{ .LogLevel }}
  formatter: text
  fields:
    service: registry
storage:
  delete:
    enabled: true
  cache:
    blobdescriptor: inmemory
  filesystem:
    rootdirectory: {{ .LocalStorageDisk }}
http:
  addr: :{{ .LocalStoragePort }}
  headers:
    X-Content-Type-Options: [nosniff]
      #auth:
      #htpasswd:
      #realm: basic-realm
      #path: /etc/registry
`

	var buff bytes.Buffer
	type RegistryConfig struct {
		LocalStorageDisk string
		LocalStoragePort int
		LogLevel         string
		LogAccessOff     bool
	}

	rc := RegistryConfig{
		LocalStorageDisk: opts.LocalStorageDisk,
		LocalStoragePort: int(opts.Port),
		LogLevel:         opts.LogLevel,
		LogAccessOff:     true,
	}

	if opts.LogLevel == "debug" || opts.LogLevel == "trace" {
		rc.LogLevel = "debug"
		rc.LogAccessOff = false
	}

	t := template.Must(template.New("local-storage-config").Parse(configYamlV01))
	err := t.Execute(&buff, rc)
	if err != nil {
		return &configuration.Configuration{}, fmt.Errorf("error parsing the config template %v", err)
	}

	config, err := configuration.Parse(bytes.NewReader(buff.Bytes()))
	if err != nil {
		return &configuration.Configuration{}, fmt.Errorf("error parsing local storage configuration : %v", err)
	}
	return config, nil
}
