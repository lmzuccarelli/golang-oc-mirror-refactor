package cli

import (
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
)

type SetupInteface interface {
	CreateDirectories() error
}

type Setup struct {
	Log     clog.PluggableLoggerInterface
	Options *common.MirrorOptions
}

func (o Setup) CreateDirectories() error {
	// ensure working dir exists
	err := os.MkdirAll(o.Options.WorkingDir, 0755)
	if err != nil {
		return fmt.Errorf("setup working-dir (%s) %v ", o.Options.WorkingDir, err)
	}

	// create signatures directory
	o.Log.Trace("creating signatures directory %s ", o.Options.WorkingDir+"/"+signaturesDir)
	err = os.MkdirAll(o.Options.WorkingDir+"/"+signaturesDir, 0755)
	if err != nil {
		return fmt.Errorf("setup working-dir for signatures %v ", err)
	}

	// create release-images directory
	o.Log.Trace("creating release images directory %s ", o.Options.WorkingDir+"/"+releaseImageDir)
	err = os.MkdirAll(o.Options.WorkingDir+"/"+releaseImageDir, 0755)
	if err != nil {
		return fmt.Errorf("setup working-dir for release images %v ", err)
	}

	// create release cache dir
	o.Log.Trace("creating release cache directory %s ", o.Options.WorkingDir+"/"+releaseImageExtractDir)
	err = os.MkdirAll(o.Options.WorkingDir+"/"+releaseImageExtractDir, 0755)
	if err != nil {
		return fmt.Errorf("setup working-dir for release cache %v ", err)
	}

	// create cincinnati graph dir
	o.Log.Trace("creating cincinnati graph data directory %s ", path.Join(o.Options.WorkingDir, releaseImageExtractDir, cincinnatiGraphDataDir))
	err = os.MkdirAll(path.Join(o.Options.WorkingDir, releaseImageExtractDir, cincinnatiGraphDataDir), 0755)
	if err != nil {
		return fmt.Errorf("setup working-dir for cincinnati graph data directory %v ", err)
	}

	//TODO ALEX REMOVE ME WHEN filtered_collector.go is the default for operators
	// create operator cache dir
	o.Log.Trace("creating operator cache directory %s ", o.Options.WorkingDir+"/"+operatorImageExtractDir)
	err = os.MkdirAll(o.Options.WorkingDir+"/"+operatorImageExtractDir, 0755)
	if err != nil {
		return fmt.Errorf("setup working-dir for operator cache %v ", err)
	}

	o.Log.Trace("creating operator cache directory %s ", filepath.Join(o.Options.WorkingDir, operatorCatalogsDir))
	err = os.MkdirAll(filepath.Join(o.Options.WorkingDir, operatorCatalogsDir), 0755)
	if err != nil {
		fmt.Errorf("setup working-dir for operator cache %v ", err)
	}

	// create cluster-resources directory and clean it
	o.Log.Trace("creating cluster-resources directory %s ", o.Options.WorkingDir+"/"+clusterResourcesDir)
	if o.Options.Mode != "delete" {
		err = os.RemoveAll(o.Options.WorkingDir + "/" + clusterResourcesDir)
		if err != nil {
			return fmt.Errorf("setup working-dir for cluster resources: failed to clear folder %s: %v ", o.Options.WorkingDir+"/"+clusterResourcesDir, err)
		}
	}
	err = os.MkdirAll(o.Options.WorkingDir+"/"+clusterResourcesDir, 0755)
	if err != nil {
		return fmt.Errorf("setup working-dir for cluster resources %v ", err)
	}

	err = os.MkdirAll(filepath.Join(o.Options.WorkingDir, helmDir, helmChartDir), 0755)
	if err != nil {
		return fmt.Errorf("setup working-dir for helm directory %v ", err)
	}

	err = os.MkdirAll(filepath.Join(o.Options.WorkingDir, helmDir, helmIndexesDir), 0755)
	if err != nil {
		return fmt.Errorf("setup working-dir for helm directory %v ", err)
	}

	err = os.MkdirAll(filepath.Join(o.Options.WorkingDir, "logs"), 0755)
	if err != nil {
		return fmt.Errorf("setup working-dir logs %v ", err)
	}

	if os.Getenv(cacheEnvVar) != "" && o.Options.CacheDir != "" {
		o.Options.CacheDir = os.Getenv(cacheEnvVar)
	} else {
		home := os.Getenv("HOME")
		o.Options.CacheDir = home + "/.oc-mirror/.cache"
	}

	err = os.MkdirAll(o.Options.CacheDir, 0755)
	if err != nil {
		return fmt.Errorf("unable to setup folder for oc-mirror cache directory: %v ", err)
	}
	o.Options.LocalStorageDisk = o.Options.CacheDir

	return nil
}
