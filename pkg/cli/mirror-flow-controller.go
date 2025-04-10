package cli

import (
	"context"
	"fmt"
	"slices"
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
	Log      clog.PluggableLoggerInterface
	Options  *common.MirrorOptions
	Validate ValidateInterface
	Setup    SetupInterface
}

func NewMirrorFlowController(log clog.PluggableLoggerInterface, opts *common.MirrorOptions, validate ValidateInterface, setup SetupInterface) MirrorFlowController {
	return MirrorFlowController{
		Log:      log,
		Options:  opts,
		Validate: validate,
		Setup:    setup,
	}
}

func (o MirrorFlowController) Process(args []string) error {
	err := o.Validate.CheckArgs(args)
	if err != nil {
		return fmt.Errorf("validation failed %s", err.Error())
	}

	o.Log.Info(emoji.WavingHandSign + " Hello, welcome to oc-mirror (version refactor)")
	o.Log.Info(emoji.Gear + "  setting up the environment for you...")

	err = o.Setup.CreateDirectories()
	if err != nil {
		return fmt.Errorf("setting up directories %s", err.Error())
	}

	o.Log.Info(emoji.TwistedRighwardsArrows+" workflow mode: %s ", o.Options.Mode)

	if o.Options.SinceString != "" {
		o.Options.Since, err = time.Parse(time.DateOnly, o.Options.SinceString)
		if err != nil {
			// this should not happen, as should be caught by Validate
			return fmt.Errorf("unable to parse since flag: %w. Expected format is yyyy-MM.dd", err)
		}
	}
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
			o.Log.Error(" %w ", err)
			return err
		}
		err = extractor.Unarchive()
		if err != nil {
			o.Log.Error(" %w ", err)
			return err
		}
	}

	// setup all dependencies
	// use interface segregation
	// use dependency inversion
	catalog := operator.NewRebuildCatalog(o.Log, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	mirror := mirror.New(o.Log, o.Options)
	collectManager := collector.New(o.Log, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	releaseCollector := release.New(o.Log, mirror, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	additionalCollector := additional.New(o.Log, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	operatorCollector := operator.New(o.Log, mirror, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	helmCollector := helm.New(o.Log, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	graph := release.NewGraphUpdate(o.Log, cfg.(v2alpha1.ImageSetConfiguration), o.Options)
	dryRun := NewDryRun(o.Log, o.Options)

	ctx := context.Background()
	localStorage := LocalStorage{Log: o.Log, Options: o.Options}
	err = localStorage.Setup()
	if err != nil {
		o.Log.Error(" %w ", err)
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
	batch := batch.New(o.Log, o.Options.WorkingDir+"/logs", mirror, 8)

	copiedImages := getUpdatedCopiedImages(allCollectorSchema)

	if o.Options.DryRun {
		err := dryRun.Process(copiedImages.AllImages)
		if err != nil {
			return err
		}
		return nil
	}
	copiedImages.AllImages, err = withMaxNestedPaths(copiedImages.AllImages, o.Options.MaxNestedPaths)
	if err != nil {
		return err
	}

	copiedImages.AllImages = excludeImages(copiedImages.AllImages, cfg.(v2alpha1.ImageSetConfiguration).Mirror.BlockedImages)

	err = catalog.Rebuild(copiedImages)
	if err != nil {
		o.Log.Warn("%v", err)
	}

	// batch all images
	_, err = batch.Worker(ctx, copiedImages, o.Options)
	if err != nil {
		return err
	}

	graphImage, err := graph.Create(graphURL)
	if err != nil {
		return err
	}

	err = o.postMirrorProcessM2D(ctx, copiedImages, allCollectorSchema, cfg.(v2alpha1.ImageSetConfiguration), graphImage)
	if err != nil {
		return err
	}

	err = o.postMirrorProcessAny2M(ctx, copiedImages, allCollectorSchema, cfg.(v2alpha1.ImageSetConfiguration), graphImage)
	if err != nil {
		return err
	}

	localStorage.StopLocalRegistry()
	o.Log.Info(emoji.WavingHandSign + " Goodbye, thank you for using oc-mirror")
	return nil
}

// postMirrorProcessM2D
func (o MirrorFlowController) postMirrorProcessM2D(ctx context.Context, copiedImages v2alpha1.CollectorSchema, allCollectorSchema []v2alpha1.CollectorSchema, cfg v2alpha1.ImageSetConfiguration, graphImage string) error {
	// post batch process
	if o.Options.IsMirrorToDisk() {
		copiedSchema, err := addRebuiltCatalogs(copiedImages)
		if err != nil {
			return err
		}
		err = o.createAndBuildArchive(ctx, copiedSchema, cfg)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (o MirrorFlowController) postMirrorProcessAny2M(ctx context.Context, copiedImages v2alpha1.CollectorSchema, allCollectorSchema []v2alpha1.CollectorSchema, cfg v2alpha1.ImageSetConfiguration, graphImage string) error {
	// drop here if disk-to-mirror or mirror-to-mirror
	// create all relevant cluster resources
	// create IDMS/ITMS
	errs := []error{}
	if o.Options.IsDiskToMirror() || o.Options.IsMirrorToMirror() {
		clusterRes := clusterresources.New(o.Log, cfg, o.Options)
		forceRepositoryScope := o.Options.MaxNestedPaths > 0
		err := clusterRes.IDMS_ITMSGenerator(copiedImages.AllImages, forceRepositoryScope)
		errs = append(errs, err)

		err = clusterRes.CatalogSourceGenerator(copiedImages.AllImages)
		errs = append(errs, err)

		err = clusterRes.ClusterCatalogGenerator(copiedImages.AllImages)
		errs = append(errs, err)

		err = clusterRes.GenerateSignatureConfigMap(copiedImages.AllImages)
		if err != nil {
			// as this is not a seriously fatal error we just log the error
			o.Log.Warn("%s", err)
		}

		err = checkAndBuildGraph(clusterRes, graphImage, allCollectorSchema)
		errs = append(errs, err)

		for _, e := range errs {
			if e != nil {
				return e
			}
		}
	}
	return nil
}

func (o MirrorFlowController) createAndBuildArchive(ctx context.Context, copiedSchema v2alpha1.CollectorSchema, cfg v2alpha1.ImageSetConfiguration) error {
	maxSize := cfg.ImageSetConfigurationSpec.ArchiveSize
	archiver, err := archive.NewPermissiveMirrorArchive(o.Options, o.Log, maxSize)
	if err != nil {
		return err
	}
	o.Log.Info(emoji.Package + " Preparing the tarball archive...")
	err = archiver.BuildArchive(ctx, copiedSchema.AllImages)
	if err != nil {
		return err
	}
	return nil
}

func checkAndBuildGraph(clusterRes clusterresources.GeneratorInterface, graphImage string, allCollectorSchema []v2alpha1.CollectorSchema) error {
	if len(graphImage) > 0 {
		releaseImage, err := findFirstRelease(allCollectorSchema)
		if err != nil {
			return err
		}
		err = clusterRes.UpdateServiceGenerator(graphImage, releaseImage.Destination)
		if err != nil {
			return err
		}
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
			case v2alpha1.TypeCincinnatiGraph, v2alpha1.TypeOCPRelease, v2alpha1.TypeOCPReleaseContent, v2alpha1.TypeKubeVirtContainer:
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
			case v2alpha1.TypeInvalid:
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
				return cs, fmt.Errorf("unable to add rebuilt catalog for %s: %w", ci.Origin, err)
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
			if img.Type == v2alpha1.TypeOCPRelease {
				return img, nil
			}
		}
	}
	return v2alpha1.CopyImageSchema{}, fmt.Errorf("no release image found")
}

// withMaxNestedPaths()
func withMaxNestedPaths(in []v2alpha1.CopyImageSchema, maxNestedPaths int) ([]v2alpha1.CopyImageSchema, error) {
	out := []v2alpha1.CopyImageSchema{}
	if maxNestedPaths > 0 {
		for _, img := range in {
			dst, err := image.WithMaxNestedPaths(img.Destination, maxNestedPaths)
			if err != nil {
				return nil, err
			}
			img.Destination = dst
			out = append(out, img)
		}
	}
	return out, nil
}

// excludeImages
func excludeImages(images []v2alpha1.CopyImageSchema, excluded []v2alpha1.Image) []v2alpha1.CopyImageSchema {
	if excluded == nil {
		return images
	}
	images = slices.DeleteFunc(images, func(image v2alpha1.CopyImageSchema) bool {
		if image.Origin == "" {
			return false
		}
		isInSlice := slices.ContainsFunc(excluded, func(excludedImage v2alpha1.Image) bool {
			imgOrigin := image.Origin
			if strings.Contains(imgOrigin, "://") {
				splittedImageOrigin := strings.Split(imgOrigin, "://")
				imgOrigin = splittedImageOrigin[1]
			}
			return excludedImage.Name == imgOrigin
		})
		return isInSlice
	})
	return images
}
