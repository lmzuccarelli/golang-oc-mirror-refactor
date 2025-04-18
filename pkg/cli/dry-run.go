package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/emoji"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/mirror"
)

type DryRunInterface interface {
	Process([]v2alpha1.CopyImageSchema) error
}

type DryRun struct {
	Log     clog.PluggableLoggerInterface
	Options *common.MirrorOptions
}

func NewDryRun(log clog.PluggableLoggerInterface, opts *common.MirrorOptions) *DryRun {
	return &DryRun{Log: log, Options: opts}
}

func (o DryRun) Process(allImages []v2alpha1.CopyImageSchema) error {
	// set up location of logs dir
	outDir := filepath.Join(o.Options.WorkingDir, dryRunOutDir)
	// clean up logs directory
	os.RemoveAll(outDir)

	// create logs directory
	err := os.MkdirAll(outDir, 0755)
	if err != nil {
		o.Log.Error(" %v ", err)
		return fmt.Errorf("creating %s %w ", outDir, err)
	}
	// creating file for storing list of cached images
	mappingTxtFilePath := filepath.Join(outDir, mappingFile)
	mappingTxtFile, err := os.Create(mappingTxtFilePath)
	if err != nil {
		return fmt.Errorf("creating mapping from dry-run %w", err)
	}
	defer mappingTxtFile.Close()

	// imagesAvailable := map[string]bool{}
	buff, nbMissingImgs := o.checkMissing(allImages)
	_, err = mappingTxtFile.Write(buff.Bytes())
	if err != nil {
		return fmt.Errorf("writing mapping file %w", err)
	}
	if nbMissingImgs > 0 {
		// creating file for storing list of cached images
		missingImgsFilePath := filepath.Join(outDir, missingImgsFile)
		missingImgsTxtFile, err := os.Create(missingImgsFilePath)
		if err != nil {
			return fmt.Errorf("creating missing mapping file %w", err)
		}
		defer missingImgsTxtFile.Close()
		_, err = missingImgsTxtFile.Write(buff.Bytes())
		if err != nil {
			return fmt.Errorf("writing missing mapping file %w", err)
		}
		o.Log.Warn(emoji.Warning+"  %d/%d images necessary for mirroring are not available in the cache.", nbMissingImgs, len(allImages))
		o.Log.Warn("List of missing images in : %s.\nplease re-run the mirror to disk process", missingImgsFilePath)
	}

	// if len(imagesAvailable) > 0 {
	//	o.Log.Info("all %d images required for mirroring are available in local cache. You may proceed with mirroring from disk to disconnected registry", len(imagesAvailable))
	// }
	o.Log.Info(emoji.PageFacingUp+" list of all images for mirroring in : %s", mappingTxtFilePath)
	return nil
}

func (o DryRun) checkMissing(allImages []v2alpha1.CopyImageSchema) (bytes.Buffer, int) {
	nbMissingImgs := 0
	var buff bytes.Buffer
	var missingImgsBuff bytes.Buffer
	ctx := context.Background()
	for _, img := range allImages {
		buff.WriteString(img.Source + "=" + img.Destination + "\n")
		if o.Options.IsMirrorToDisk() {
			exists, err := mirror.Check(ctx, img.Destination, o.Options, false)
			if err != nil {
				o.Log.Debug("unable to check existence of %s in local cache: %w", img.Destination, err)
			}
			if err != nil || !exists {
				missingImgsBuff.WriteString(img.Source + "=" + img.Destination + "\n")
				nbMissingImgs++
			}
		}
	}
	return missingImgsBuff, nbMissingImgs
}
