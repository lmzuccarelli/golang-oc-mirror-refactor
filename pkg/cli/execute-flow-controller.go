package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/additional"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/archive"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/batch"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/collector"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/config"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/emoji"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/mirror"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/release"
)

type ExecuteFlowControllerInterface interface {
	MirrorProcess(args []string) error
	DeleteProcess(args []string) error
}

type ExecuteFlowController struct {
	Log     clog.PluggableLoggerInterface
	Options *common.MirrorOptions
	Context context.Context
}

func NewExecuteFlowController(ctx context.Context, log clog.PluggableLoggerInterface, opts *common.MirrorOptions) ExecuteFlowControllerInterface {
	return ExecuteFlowController{
		Context: ctx,
		Log:     log,
		Options: opts,
	}
}

func (o ExecuteFlowController) DeleteProcess(args []string) error {
	return nil
}

func (o ExecuteFlowController) MirrorProcess(args []string) error {
	validate := Validate{Log: o.Log, Options: o.Options}
	err := validate.CheckArgs(args)
	if err != nil {
		return fmt.Errorf("validation failed %s", err.Error())
	}

	o.Log.Info(emoji.WavingHandSign + " Hello, welcome to oc-mirror")
	o.Log.Info(emoji.Gear + "  setting up the environment for you...")

	setup := Setup{Log: o.Log, Options: o.Options}
	err = setup.CreateDirectories()
	if err != nil {
		return fmt.Errorf("setting up directories %s", err.Error())
	}

	o.Log.Info(emoji.TwistedRighwardsArrows+" workflow mode: %s ", o.Options.Mode)

	if o.Options.SinceString != "" {
		o.Options.Since, err = time.Parse(time.DateOnly, o.Options.SinceString)
		if err != nil {
			// this should not happen, as should be caught by Validate
			return fmt.Errorf("unable to parse since flag: %v. Expected format is yyyy-MM.dd", err)
		}
	}
	o.Options.Function = "mirror"

	config := config.Config{}
	cfg, err := config.Read(o.Options.ConfigPath, v2alpha1.ImageSetConfigurationKind)
	if err != nil {
		return err
	}

	mirror := mirror.New(o.Context, o.Log, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	collectManager := collector.New(o.Context, o.Log, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	release := release.New(o.Context, o.Log, mirror, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	additional := additional.New(o.Context, o.Log, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	collectManager.AddCollector(release)
	collectManager.AddCollector(additional)
	allImages, err := collectManager.CollectAllImages()
	if err != nil {
		return err
	}
	o.Log.Trace("source %v", allImages)
	batch := batch.New(o.Log, o.Options.WorkingDir+"/logs", mirror, 8)

	localStorage := LocalStorage{Log: o.Log, Options: o.Options, Context: o.Context}
	localStorage.Setup()
	go localStorage.StartLocalRegistry()

	// batch all images
	copiedImages := getUpdatedCopiedImages(allImages)

	_, err = batch.Worker(o.Context, copiedImages, o.Options)
	if err != nil {
		return err
	}
	if o.Options.IsMirrorToDisk() {
		maxSize := cfg.(v2alpha1.ImageSetConfiguration).ImageSetConfigurationSpec.ArchiveSize
		archiver, err := archive.NewPermissiveMirrorArchive(o.Options, o.Log, maxSize)
		if err != nil {
			return err
		}
		// prepare tar.gz when mirror to disk
		o.Log.Info(emoji.Package + " Preparing the tarball archive...")
		// next, generate the archive
		err = archiver.BuildArchive(o.Context, copiedImages.AllImages)
		if err != nil {
			return err
		}
		return nil
	}

	localStorage.StopLocalRegistry()
	o.Log.Info(emoji.WavingHandSign + " Goodbye, thank you for using oc-mirror")
	return nil
}

// utility helper functions
// getUpdatedCopiedImages
func getUpdatedCopiedImages(images []v2alpha1.CopyImageSchema) v2alpha1.CollectorSchema {
	result := v2alpha1.CollectorSchema{}
	for _, img := range images {
		switch img.Type {
		case v2alpha1.TypeCincinnatiGraph, v2alpha1.TypeOCPRelease, v2alpha1.TypeOCPReleaseContent:
			result.TotalReleaseImages++
		case v2alpha1.TypeGeneric:
			result.TotalAdditionalImages++
		case v2alpha1.TypeOperatorBundle, v2alpha1.TypeOperatorCatalog, v2alpha1.TypeOperatorRelatedImage:
			result.TotalOperatorImages++
		case v2alpha1.TypeHelmImage:
			result.TotalHelmImages++
		}
	}
	result.AllImages = images
	return result
}
