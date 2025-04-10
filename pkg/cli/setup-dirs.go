package cli

import (
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
)

type SetupInterface interface {
	CreateDirectories() error
}

type Setup struct {
	Log     clog.PluggableLoggerInterface
	Options *common.MirrorOptions
}

func (o Setup) CreateDirectories() error {

	if os.Getenv(cacheEnvVar) != "" && o.Options.CacheDir != "" {
		o.Options.CacheDir = os.Getenv(cacheEnvVar)
	} else {
		home := os.Getenv("HOME")
		o.Options.CacheDir = home + "/.oc-mirror/.cache"
	}

	// ensure working dir exists
	createDirs := []string{
		o.Options.WorkingDir,
		o.Options.WorkingDir + "/" + signaturesDir,
		o.Options.WorkingDir + "/" + releaseImageDir,
		o.Options.WorkingDir + "/" + releaseImageExtractDir,
		path.Join(o.Options.WorkingDir, releaseImageExtractDir, cincinnatiGraphDataDir),
		o.Options.WorkingDir + "/" + operatorImageExtractDir,
		filepath.Join(o.Options.WorkingDir, operatorCatalogsDir),
		o.Options.WorkingDir + "/" + clusterResourcesDir,
		filepath.Join(o.Options.WorkingDir, helmDir, helmChartDir),
		filepath.Join(o.Options.WorkingDir, helmDir, helmIndexesDir),
		filepath.Join(o.Options.WorkingDir, "logs"),
		o.Options.CacheDir,
	}

	if o.Options.Mode != "delete" {
		err := os.RemoveAll(o.Options.WorkingDir + "/" + clusterResourcesDir)
		if err != nil {
			return fmt.Errorf("deleting folder %s %w", o.Options.WorkingDir+"/"+clusterResourcesDir, err)
		}
	}

	for _, dir := range createDirs {
		o.Log.Trace("creating directory %s ", dir)
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			return fmt.Errorf("creating setup directory (%s) %w ", dir, err)
		}
	}
	o.Options.LocalStorageDisk = o.Options.CacheDir
	return nil
}
