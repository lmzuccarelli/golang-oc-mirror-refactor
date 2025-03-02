package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/additional"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/archive"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/batch"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/clusterresources"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/collector"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/config"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/emoji"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/helm"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/image"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/mirror"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/operator"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/release"
)

type MirrorFlowController struct {
	Log     clog.PluggableLoggerInterface
	Options *common.MirrorOptions
	Context context.Context
}

func NewMirrorFlowController(ctx context.Context, log clog.PluggableLoggerInterface, opts *common.MirrorOptions) FlowControllerInterface {
	return MirrorFlowController{
		Context: ctx,
		Log:     log,
		Options: opts,
	}
}

func (o MirrorFlowController) Process(args []string) error {
	validate := MirrorValidate{Log: o.Log, Options: o.Options}
	err := validate.CheckArgs(args)
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

	if o.Options.IsDiskToMirror() {
		archiveBaseDir := o.Options.WorkingDir
		if strings.Contains(o.Options.WorkingDir, "working-dir") {
			archiveBaseDir = strings.Split(o.Options.WorkingDir, "working-dir")[0]
		}
		// extract the archive
		extractor, err := archive.NewArchiveExtractor(archiveBaseDir, archiveBaseDir, o.Options.LocalStorageDisk)
		if err != nil {
			o.Log.Error(" %v ", err)
			return err
		}
		err = extractor.Unarchive()
		if err != nil {
			o.Log.Error(" %v ", err)
			return err
		}
	}

	// setup all dependencies
	// use interface segregation
	// use dependency inversion
	catalog := operator.NewRebuildCatalog(o.Context, o.Log, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	mirror := mirror.New(o.Context, o.Log, o.Options)
	collectManager := collector.New(o.Context, o.Log, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	releaseCollector := release.New(o.Context, o.Log, mirror, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	additionalCollector := additional.New(o.Context, o.Log, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	operatorCollector := operator.New(o.Context, o.Log, mirror, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	helmCollector := helm.New(o.Context, o.Log, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	clusterRes := clusterresources.New(o.Log, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	graph := release.NewGraphUpdate(o.Context, o.Log, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	dryRun := NewDryRun(o.Context, o.Log, o.Options)

	localStorage := LocalStorage{Log: o.Log, Options: o.Options, Context: o.Context}
	localStorage.Setup()

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
	batch := batch.New(o.Log, o.Options.WorkingDir+"/logs", mirror, 8)

	copiedImages := getUpdatedCopiedImages(allCollectorSchema)

	if o.Options.DryRun {
		err := dryRun.Process(copiedImages.AllImages)
		if err != nil {
			return nil
		}
	} else {

		err = catalog.Rebuild(copiedImages)
		if err != nil {
			o.Log.Warn("%v", err)
		}

		// batch all images
		_, err = batch.Worker(o.Context, copiedImages, o.Options)
		if err != nil {
			return err
		}

		graphImage, err := graph.Create(graphURL)
		if err != nil {
			return err
		}

		// post batch process
		if o.Options.IsMirrorToDisk() {
			copiedSchema, err := addRebuiltCatalogs(copiedImages)
			if err != nil {
				return err
			}
			maxSize := cfg.(v2alpha1.ImageSetConfiguration).ImageSetConfigurationSpec.ArchiveSize
			archiver, err := archive.NewPermissiveMirrorArchive(o.Options, o.Log, maxSize)
			if err != nil {
				return err
			}
			o.Log.Info(emoji.Package + " Preparing the tarball archive...")
			err = archiver.BuildArchive(o.Context, copiedSchema.AllImages)
			if err != nil {
				return err
			}
			return nil
		} else {
			// create all relevant cluster resources
			// create IDMS/ITMS
			forceRepositoryScope := o.Options.MaxNestedPaths > 0
			err = clusterRes.IDMS_ITMSGenerator(copiedImages.AllImages, forceRepositoryScope)
			if err != nil {
				return err
			}

			err = clusterRes.CatalogSourceGenerator(copiedImages.AllImages)
			if err != nil {
				return err
			}

			if err := clusterRes.ClusterCatalogGenerator(copiedImages.AllImages); err != nil {
				return err
			}

			err = clusterRes.GenerateSignatureConfigMap(copiedImages.AllImages)
			if err != nil {
				// as this is not a seriously fatal error we just log the error
				o.Log.Warn("%s", err)
			}

			if len(graphImage) > 0 {
				releaseImage, err := findFirstRelease(allCollectorSchema)
				if err != nil {
					return err
				}
				err = clusterRes.UpdateServiceGenerator(graphImage, releaseImage.Destination)
				if err != nil {
					return err
				}
			}

		}

		localStorage.StopLocalRegistry()
		o.Log.Info(emoji.WavingHandSign + " Goodbye, thank you for using oc-mirror")
		return nil
	}
	return nil
}

// utility helper functions
// TODO: refactor these utilities in their appropriate pkgs
// getUpdatedCopiedImages
func getUpdatedCopiedImages(cs []v2alpha1.CollectorSchema) v2alpha1.CollectorSchema {
	result := v2alpha1.CollectorSchema{}
	for _, v := range cs {
		for _, img := range v.AllImages {
			switch img.Type {
			case v2alpha1.TypeCincinnatiGraph, v2alpha1.TypeOCPRelease, v2alpha1.TypeOCPReleaseContent:
				result.TotalReleaseImages++
			case v2alpha1.TypeGeneric:
				result.TotalAdditionalImages++
			case v2alpha1.TypeOperatorBundle, v2alpha1.TypeOperatorCatalog, v2alpha1.TypeOperatorRelatedImage:
				result.CatalogToFBCMap = v.CatalogToFBCMap
				result.CopyImageSchemaMap = v.CopyImageSchemaMap
				result.CopyImageSchemaMap.BundlesByImage = v.CopyImageSchemaMap.BundlesByImage
				result.CopyImageSchemaMap.OperatorsByImage = v.CopyImageSchemaMap.OperatorsByImage
				result.TotalOperatorImages++
			case v2alpha1.TypeHelmImage:
				result.TotalHelmImages++
			}
		}
		result.AllImages = append(result.AllImages, v.AllImages...)
	}
	return result
}

// addRebuiltCatalogs
func addRebuiltCatalogs(cs v2alpha1.CollectorSchema) (v2alpha1.CollectorSchema, error) {
	for _, ci := range cs.AllImages {
		if ci.Type == v2alpha1.TypeOperatorCatalog && ci.RebuiltTag != "" {
			imgSpec, err := image.ParseRef(ci.Destination)
			if err != nil {
				return cs, fmt.Errorf("unable to add rebuilt catalog for %s: %v", ci.Origin, err)
			}
			imgSpec = imgSpec.SetTag(ci.RebuiltTag)
			rebuiltCI := v2alpha1.CopyImageSchema{
				Origin:      ci.Origin,
				Source:      imgSpec.ReferenceWithTransport,
				Destination: imgSpec.ReferenceWithTransport,
				Type:        v2alpha1.TypeOperatorCatalog,
			}
			cs.AllImages = append(cs.AllImages, rebuiltCI)
		}
	}
	return cs, nil
}

// findFirstRelease
func findFirstRelease(cs []v2alpha1.CollectorSchema) (v2alpha1.CopyImageSchema, error) {
	for _, v := range cs {
		for _, img := range v.AllImages {
			switch img.Type {
			case v2alpha1.TypeOCPRelease:
				return img, nil
			}
		}
	}
	return v2alpha1.CopyImageSchema{}, fmt.Errorf("no release image found")
}
