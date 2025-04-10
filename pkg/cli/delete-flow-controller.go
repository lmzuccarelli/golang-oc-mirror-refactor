package cli

import (
	"fmt"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/additional"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/archive"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/batch"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/collector"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/config"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/delete"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/emoji"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/helm"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/mirror"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/operator"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/release"
)

type DeleteFlowController struct {
	Log      clog.PluggableLoggerInterface
	Options  *common.MirrorOptions
	Validate ValidateInterface
	Setup    SetupInterface
}

func NewDeleteFlowController(log clog.PluggableLoggerInterface, opts *common.MirrorOptions, validate ValidateInterface, setup SetupInterface) DeleteFlowController {
	return DeleteFlowController{
		Log:      log,
		Options:  opts,
		Validate: validate,
		Setup:    setup,
	}
}

func (o DeleteFlowController) Process(args []string) error {
	err := o.Validate.CheckArgs(args)
	if err != nil {
		return fmt.Errorf("validation failed %s", err.Error())
	}

	o.Log.Info(emoji.WavingHandSign + " Hello, welcome to oc-mirror (version refactor)")
	o.Log.Info(emoji.Gear + "  setting up the environment for you...")

	setup := Setup{Log: o.Log, Options: o.Options}
	err = setup.CreateDirectories()
	if err != nil {
		return fmt.Errorf("setting up directories %s", err.Error())
	}

	config := config.Config{}
	cfg, err := config.Read(o.Options.ConfigPath, v2alpha1.DeleteImageSetConfigurationKind)
	if err != nil {
		return err
	}
	isc := v2alpha1.ImageSetConfiguration{
		ImageSetConfigurationSpec: v2alpha1.ImageSetConfigurationSpec{
			Mirror: v2alpha1.Mirror{
				Platform:         cfg.(v2alpha1.DeleteImageSetConfiguration).Delete.Platform,
				Operators:        cfg.(v2alpha1.DeleteImageSetConfiguration).Delete.Operators,
				AdditionalImages: cfg.(v2alpha1.DeleteImageSetConfiguration).Delete.AdditionalImages,
				Helm:             cfg.(v2alpha1.DeleteImageSetConfiguration).Delete.Helm,
			},
		},
	}

	// ensure mirror and batch worker use delete logic
	o.Log.Info(emoji.TwistedRighwardsArrows+" workflow mode: %s / %s", o.Options.Mode, o.Options.Function)

	/*
		if o.Options.DeleteGenerate {
			absPath, err := filepath.Abs(o.Options.WorkingDir + deleteDir)
			if err != nil {
				o.Log.Error("absolute path %v", err)
			}
			if len(o.Options.DeleteID) > 0 {
				o.Log.Debug("using id %s to update all delete generated files", o.Options.DeleteID)
			}
			o.Log.Debug("generate flag set, files will be created in %s", absPath)
		}

		if o.Options.ForceCacheDelete && !o.Options.DeleteGenerate {
			o.Log.Debug("force-cache-delete flag set, cache deletion will be forced")
		}
	*/

	mirror := mirror.New(o.Log, o.Options)
	batch := batch.New(o.Log, o.Options.WorkingDir+"/logs", mirror, 8)
	bg := archive.NewImageBlobGatherer(o.Options)
	collectManager := collector.New(o.Log, isc, o.Options)
	releaseCollector := release.New(o.Log, mirror, isc, o.Options)
	additionalCollector := additional.New(o.Log, isc, o.Options)
	operatorCollector := operator.New(o.Log, mirror, isc, o.Options)
	helmCollector := helm.New(o.Log, isc, o.Options)
	deleteReg := delete.New(o.Log, o.Options, batch, bg, cfg.(v2alpha1.DeleteImageSetConfiguration))

	localStorage := LocalStorage{Log: o.Log, Options: o.Options}
	err = localStorage.Setup()
	if err != nil {
		return err
	}

	go localStorage.StartLocalRegistry()

	// use single responsibility principle
	// use of open/close principle
	collectManager.AddCollector(releaseCollector)
	collectManager.AddCollector(additionalCollector)
	collectManager.AddCollector(operatorCollector)
	collectManager.AddCollector(helmCollector)
	allCollectorSchema, err := collectManager.CollectAllImages()
	if err != nil {
		return err
	}
	o.Log.Trace("source %v", allCollectorSchema)
	copiedImages := getUpdatedCopiedImages(allCollectorSchema)

	if o.Options.DeleteGenerate {
		err = deleteReg.WriteDeleteMetaData(copiedImages.AllImages)
		if err != nil {
			o.Log.Error("%v", err)
			return err
		}
	} else {
		images, err := deleteReg.ReadDeleteMetaData()
		if err != nil {
			o.Log.Error("%v", err)
			return err
		}
		err = deleteReg.DeleteRegistryImages(images)
		if err != nil {
			o.Log.Error("%v", err)
			return err
		}
	}

	localStorage.StopLocalRegistry()
	o.Log.Info(emoji.WavingHandSign + " Goodbye, thank you for using oc-mirror")
	return nil
}
