package operator

import (
	"context"
	// #nosec G501
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/containers/image/v5/types"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/emoji"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/image"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/manifest"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/mirror"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/spinners"
	"github.com/opencontainers/go-digest"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/otiai10/copy"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

const (
	operatorImageExtractDir           = "hold-operator" // TODO ALEX REMOVE ME when filtered_collector.go is the default
	dockerProtocol                    = "docker://"
	ociProtocol                       = "oci://"
	ociProtocolTrimmed                = "oci:"
	operatorImageDir                  = "operator-images" // TODO ALEX REMOVE ME when filtered_collector.go is the default
	operatorCatalogsDir        string = "operator-catalogs"
	operatorCatalogConfigDir   string = "catalog-config"
	operatorCatalogImageDir    string = "catalog-image"
	operatorCatalogFilteredDir string = "filtered-catalogs"
	blobsDir                          = "blobs/sha256"
	collectorPrefix                   = "[OperatorImageCollector] "
	errMsg                            = collectorPrefix + "%s"
	logsFile                          = "operator.log"
	errorSemver                string = " semver %v "
	filteredCatalogDir                = "filtered-operator"
	digestIncorrectMessage     string = "the digests seem to be incorrect for %s: %s "
)

type CollectOperator struct {
	Log     clog.PluggableLoggerInterface
	Options *common.MirrorOptions
	Config  v2alpha1.ImageSetConfiguration
	Mirror  mirror.MirrorInterface
}

func New(log clog.PluggableLoggerInterface, mi mirror.MirrorInterface, cfg v2alpha1.ImageSetConfiguration, opts *common.MirrorOptions) CollectOperator {
	return CollectOperator{
		Log:     log,
		Options: opts,
		Config:  cfg,
		Mirror:  mi,
	}
}

// OperatorImageCollector - this looks into the operator index image
// taking into account the mode we are in (mirrorToDisk, diskToMirror)
// the image is downloaded (oci format) and the index.json is inspected
// once unmarshalled, the links to manifests are inspected
func (o CollectOperator) Collect() (v2alpha1.CollectorSchema, error) {

	var (
		allImages       []v2alpha1.CopyImageSchema
		label           string
		catalogImageDir string
		catalogName     string
		rebuiltTag      string
	)
	o.Log.Debug(collectorPrefix+"setting copy option o.Opts.MultiArch=%s when collecting operator images", o.Options.MultiArch)

	hndl := catalogHandler{Log: o.Log}
	oi := Operator{
		Log:         o.Log,
		Options:     o.Options,
		Mirror:      o.Mirror,
		ctlgHandler: hndl,
		Config:      o.Config,
	}

	ctx := context.Background()

	relatedImages := make(map[string][]v2alpha1.RelatedImage)
	collectorSchema := v2alpha1.CollectorSchema{}
	copyImageSchemaMap := &v2alpha1.CopyImageSchemaMap{OperatorsByImage: make(map[string]map[string]struct{}), BundlesByImage: make(map[string]map[string]string)}
	cs := v2alpha1.CollectorSchema{}

	for _, op := range o.Config.Mirror.Operators {
		var catalogImage string
		// download the operator index image
		o.Log.Debug(collectorPrefix+"copying operator image %s", op.Catalog)

		if !o.Options.IsTerminal() {
			o.Log.Debug("Collecting catalog %s", op.Catalog)
		}
		// prepare spinner
		p := mpb.New(mpb.ContainerOptional(mpb.WithOutput(io.Discard), !o.Options.IsTerminal()))
		spinner := p.AddSpinner(
			1, mpb.BarFillerMiddleware(spinners.PositionSpinnerLeft),
			mpb.BarWidth(3),
			mpb.PrependDecorators(
				decor.OnComplete(spinners.EmptyDecorator(), emoji.SpinnerCheckMark),
				decor.OnAbort(spinners.EmptyDecorator(), emoji.SpinnerCrossMark),
			),
			mpb.AppendDecorators(
				decor.Name("("),
				decor.Elapsed(decor.ET_STYLE_GO),
				decor.Name(") Collecting catalog "+op.Catalog+" "),
			),
			mpb.BarFillerClearOnComplete(),
			spinners.BarFillerClearOnAbort(),
		)
		// CLID-47 double check that targetCatalog is valid
		if op.TargetCatalog != "" && !v2alpha1.IsValidPathComponent(op.TargetCatalog) {
			o.Log.Error(collectorPrefix+"invalid targetCatalog %s", op.TargetCatalog)
			spinner.Abort(true)
			spinner.Wait()
			return cs, fmt.Errorf(collectorPrefix+"invalid targetCatalog %s", op.TargetCatalog)
		}
		// CLID-27 ensure we pick up oci:// (on disk) catalogs
		imgSpec, err := image.ParseRef(op.Catalog)
		if err != nil {
			o.Log.Error(errMsg, err.Error())
			spinner.Abort(true)
			spinner.Wait()
			return cs, err
		}
		//OCPBUGS-36214: For diskToMirror (and delete), access to the source registry is not guaranteed
		catalogDigest := ""
		if o.Options.IsDiskToMirror() || o.Options.IsDelete() {
			d, err := oi.catalogDigest(ctx, op)
			if err != nil {
				o.Log.Error(errMsg, err.Error())
				spinner.Abort(true)
				spinner.Wait()
				return cs, err
			}
			catalogDigest = d
		} else {
			sourceCtx := o.Options.NewSystemContext()
			d, err := manifest.GetDigest(ctx, sourceCtx, imgSpec.ReferenceWithTransport)
			// OCPBUGS-36548 (manifest unknown)
			if err != nil {
				spinner.Abort(true)
				spinner.Wait()
				o.Log.Warn(collectorPrefix+"catalog %s : SKIPPING", err.Error())
				continue
			}
			catalogDigest = d
		}

		imageIndex := filepath.Join(imgSpec.ComponentName(), catalogDigest)
		imageIndexDir := filepath.Join(o.Options.WorkingDir, operatorCatalogsDir, imageIndex)
		configsDir := filepath.Join(imageIndexDir, operatorCatalogConfigDir)
		catalogImageDir = filepath.Join(imageIndexDir, operatorCatalogImageDir)
		filteredCatalogsDir := filepath.Join(imageIndexDir, operatorCatalogFilteredDir)

		err = createFolders([]string{configsDir, catalogImageDir, filteredCatalogsDir})
		if err != nil {
			o.Log.Error(errMsg, err.Error())
			spinner.Abort(true)
			spinner.Wait()
			return cs, err
		}

		var filteredDC *declcfg.DeclarativeConfig
		var isAlreadyFiltered bool

		filterDigest, err := digestOfFilter(op)
		if err != nil {
			spinner.Abort(true)
			spinner.Wait()
			return cs, err
		}
		rebuiltTag = filterDigest
		var srcFilteredCatalog string
		filterPath := filepath.Join(filteredCatalogsDir, filterDigest, "digest")
		filteredImageDigest, err := os.ReadFile(filterPath)
		if err == nil && len(filterDigest) > 0 {
			srcFilteredCatalog, err = oi.cachedCatalog(op, filterDigest)
			if err != nil {
				o.Log.Error(errMsg, err.Error())
				spinner.Abort(true)
				spinner.Wait()
				return cs, err
			}
			isAlreadyFiltered = oi.isAlreadyFiltered(ctx, srcFilteredCatalog, string(filteredImageDigest))
		}

		if isAlreadyFiltered {
			filterConfigDir := filepath.Join(filteredCatalogsDir, filterDigest, operatorCatalogConfigDir)
			filteredDC, err = oi.ctlgHandler.getDeclarativeConfig(filterConfigDir)
			if err != nil {
				o.Log.Error(errMsg, err.Error())
				spinner.Abort(true)
				spinner.Wait()
				return cs, err
			}
			if len(op.TargetCatalog) > 0 {
				catalogName = op.TargetCatalog
			} else {
				catalogName = path.Base(imgSpec.Reference)
			}
			if imgSpec.Transport == ociProtocol {
				// ensure correct oci format and directory lookup
				sourceOCIDir, err := filepath.Abs(imgSpec.Reference)
				if err != nil {
					o.Log.Error(errMsg, err.Error())
					return cs, fmt.Errorf("%w", err)
				}
				catalogImage = ociProtocol + sourceOCIDir
			} else {
				catalogImage = op.Catalog
			}
			catalogDigest = string(filteredImageDigest)
			if collectorSchema.CatalogToFBCMap == nil {
				collectorSchema.CatalogToFBCMap = make(map[string]v2alpha1.CatalogFilterResult)
			}
			result := v2alpha1.CatalogFilterResult{
				OperatorFilter:     op,
				FilteredConfigPath: filterConfigDir,
				ToRebuild:          false,
			}
			collectorSchema.CatalogToFBCMap[imgSpec.ReferenceWithTransport] = result

		} else {
			toRebuild := true
			if imgSpec.Transport == ociProtocol {
				if _, err := os.Stat(filepath.Join(catalogImageDir, "index.json")); errors.Is(err, os.ErrNotExist) {
					// delete the existing directory and untarred cache contents
					os.RemoveAll(catalogImageDir)
					os.RemoveAll(configsDir)
					// copy all contents to the working dir
					err := copy.Copy(imgSpec.PathComponent, catalogImageDir)
					if err != nil {
						o.Log.Error(errMsg, err.Error())
						spinner.Abort(true)
						spinner.Wait()
						return cs, fmt.Errorf("%w", err)
					}
				}

				if len(op.TargetCatalog) > 0 {
					catalogName = op.TargetCatalog
				} else {
					catalogName = path.Base(imgSpec.Reference)
				}
			} else {
				src := dockerProtocol + op.Catalog
				dest := ociProtocolTrimmed + catalogImageDir

				optsCopy := o.Options
				optsCopy.Stdout = io.Discard

				err = o.Mirror.Copy(ctx, src, dest, o.Options)

				if err != nil {
					o.Log.Error(errMsg, err.Error())
				}
			}

			// it's in oci format so we can go directly to the index.json file
			oci, err := manifest.GetImageIndex(catalogImageDir)
			if err != nil {
				o.Log.Error(errMsg, err.Error())
				spinner.Abort(true)
				spinner.Wait()
				return cs, err
			}

			if isMultiManifestIndex(*oci) && imgSpec.Transport == ociProtocol {
				err = manifest.ConvertIndexToSingleManifest(catalogImageDir, oci)
				if err != nil {
					o.Log.Error(errMsg, err.Error())
					spinner.Abort(true)
					spinner.Wait()
					return cs, fmt.Errorf("%w", err)
				}

				oci, err = manifest.GetImageIndex(catalogImageDir)
				if err != nil {
					o.Log.Error(errMsg, err.Error())
					spinner.Abort(true)
					spinner.Wait()
					return cs, fmt.Errorf("%w", err)
				}

				sourceOCIDir, err := filepath.Abs(imgSpec.Reference)
				if err != nil {
					o.Log.Error(errMsg, err.Error())
					return cs, fmt.Errorf("%w", err)
				}
				catalogImage = ociProtocol + sourceOCIDir
			} else {
				catalogImage = op.Catalog
			}

			if len(oci.Manifests) == 0 {
				o.Log.Error(collectorPrefix+"no manifests found for %s ", op.Catalog)
				spinner.Abort(true)
				spinner.Wait()
				return cs, fmt.Errorf(collectorPrefix+"no manifests found for %s ", op.Catalog)
			}

			validDigest, err := digest.Parse(oci.Manifests[0].Digest)
			if err != nil {
				o.Log.Error(collectorPrefix+digestIncorrectMessage, op.Catalog, err.Error())
				spinner.Abort(true)
				spinner.Wait()
				return cs, fmt.Errorf(collectorPrefix+"the digests seem to be incorrect for %s: %s ", op.Catalog, err.Error())
			}

			mnfst := validDigest.Encoded()
			o.Log.Debug(collectorPrefix+"manifest %s", mnfst)
			// read the operator image manifest
			manifestDir := filepath.Join(catalogImageDir, blobsDir, mnfst)
			oci, err = manifest.GetImageManifest(manifestDir)
			if err != nil {
				o.Log.Error(errMsg, err.Error())
				spinner.Abort(true)
				spinner.Wait()
				return cs, fmt.Errorf("%w", err)
			}

			// we need to check if oci returns multi manifests
			// (from manifest list) also oci.Config will be nil
			// we are only interested in the first manifest as all
			// architecture "configs" will be exactly the same
			if len(oci.Manifests) > 1 && oci.Config.Size == 0 {
				subDigest, err := digest.Parse(oci.Manifests[0].Digest)
				if err != nil {
					o.Log.Error(collectorPrefix+digestIncorrectMessage, op.Catalog, err.Error())
					spinner.Abort(true)
					spinner.Wait()
					return cs, fmt.Errorf(collectorPrefix+"the digests seem to be incorrect for %s: %s ", op.Catalog, err.Error())
				}
				manifestDir := filepath.Join(catalogImageDir, blobsDir, subDigest.Encoded())
				oci, err = manifest.GetImageManifest(manifestDir)
				if err != nil {
					o.Log.Error(collectorPrefix+"manifest %s: %s ", op.Catalog, err.Error())
					spinner.Abort(true)
					spinner.Wait()
					return cs, fmt.Errorf(collectorPrefix+"manifest %s: %s ", op.Catalog, err.Error())
				}
			}

			// read the config digest to get the detailed manifest
			// looking for the lable to search for a specific folder
			configDigest, err := digest.Parse(oci.Config.Digest)
			if err != nil {
				o.Log.Error(collectorPrefix+digestIncorrectMessage, op.Catalog, err.Error())
				spinner.Abort(true)
				spinner.Wait()
				return cs, fmt.Errorf(collectorPrefix+"the digests seem to be incorrect for %s: %s ", op.Catalog, err.Error())
			}
			catalogDir := filepath.Join(catalogImageDir, blobsDir, configDigest.Encoded())
			ocs, err := manifest.GetOperatorConfig(catalogDir)
			if err != nil {
				o.Log.Error(errMsg, err.Error())
				spinner.Abort(true)
				spinner.Wait()
				return cs, fmt.Errorf("%w", err)
			}

			label = ocs.Config.Labels.OperatorsOperatorframeworkIoIndexConfigsV1
			o.Log.Debug(collectorPrefix+"label %s", label)

			// untar all the blobs for the operator
			// if the layer with "label (from previous step) is found to a specific folder"
			fromDir := strings.Join([]string{catalogImageDir, blobsDir}, "/")
			err = manifest.ExtractLayersOCI(fromDir, configsDir, label, oci)
			if err != nil {
				spinner.Abort(true)
				spinner.Wait()
				return cs, fmt.Errorf("%w", err)
			}

			originalDC, err := oi.ctlgHandler.getDeclarativeConfig(filepath.Join(configsDir, label))
			if err != nil {
				spinner.Abort(true)
				spinner.Wait()
				return cs, fmt.Errorf("%w", err)
			}

			if !isFullCatalog(op) {

				var filteredDigestPath string
				var filterDigest string

				filteredDC, err = filterCatalog(ctx, *originalDC, op)
				if err != nil {
					spinner.Abort(true)
					spinner.Wait()
					return cs, fmt.Errorf("%w", err)
				}

				filterDigest, err = digestOfFilter(op)
				if err != nil {
					o.Log.Error(errMsg, err.Error())
					spinner.Abort(true)
					spinner.Wait()
					return cs, fmt.Errorf("%w", err)
				}

				if filterDigest != "" {
					filteredDigestPath = filepath.Join(filteredCatalogsDir, filterDigest, operatorCatalogConfigDir)

					err = createFolders([]string{filteredDigestPath})
					if err != nil {
						o.Log.Error(errMsg, err.Error())
						spinner.Abort(true)
						spinner.Wait()
						return cs, fmt.Errorf("%w", err)
					}
				}

				err = saveDeclarativeConfig(*filteredDC, filteredDigestPath)
				if err != nil {
					spinner.Abort(true)
					spinner.Wait()
					return cs, fmt.Errorf("%w", err)
				}

				if collectorSchema.CatalogToFBCMap == nil {
					collectorSchema.CatalogToFBCMap = make(map[string]v2alpha1.CatalogFilterResult)
				}
				result := v2alpha1.CatalogFilterResult{
					OperatorFilter:     op,
					FilteredConfigPath: filteredDigestPath,
					ToRebuild:          toRebuild,
				}
				collectorSchema.CatalogToFBCMap[imgSpec.ReferenceWithTransport] = result

			} else {
				rebuiltTag = ""
				toRebuild = false
				filteredDC = originalDC
				if collectorSchema.CatalogToFBCMap == nil {
					collectorSchema.CatalogToFBCMap = make(map[string]v2alpha1.CatalogFilterResult)
				}
				result := v2alpha1.CatalogFilterResult{
					OperatorFilter:     op,
					FilteredConfigPath: "", // this value is not relevant: no rebuilding required
					ToRebuild:          toRebuild,
				}
				collectorSchema.CatalogToFBCMap[imgSpec.ReferenceWithTransport] = result
			}
		}

		ri, err := oi.ctlgHandler.getRelatedImagesFromCatalog(filteredDC, copyImageSchemaMap)
		if err != nil {
			if len(ri) == 0 {
				spinner.Abort(true)
				spinner.Wait()
				continue
			}
		}

		// OCPBUGS-45059
		// TODO remove me when the migration from oc-mirror v1 to v2 ends
		if imgSpec.Transport == ociProtocol && oi.isDeleteOfV1CatalogFromDisk() {
			addOriginFromOperatorCatalogOnDisk(&ri)
		}

		maps.Copy(relatedImages, ri)

		var targetTag string
		var targetCatalog string
		if len(op.TargetTag) > 0 {
			targetTag = op.TargetTag
		} else if imgSpec.Transport == ociProtocol {
			// for this case only, img.ParseRef(in its current state)
			// will not be able to determine the digest.
			// this leaves the oci imgSpec with no tag nor digest as it
			// goes to prepareM2DCopyBatch/prepareD2MCopyBath. This is
			// why we set the digest read from manifest in targetTag
			targetTag = "latest"
		}

		if len(op.TargetCatalog) > 0 {
			targetCatalog = op.TargetCatalog

		}

		componentName := imgSpec.ComponentName() + "." + catalogDigest

		relatedImages[componentName] = []v2alpha1.RelatedImage{
			{
				Name:          catalogName,
				Image:         catalogImage,
				Type:          v2alpha1.TypeOperatorCatalog,
				TargetTag:     targetTag,
				TargetCatalog: targetCatalog,
				RebuiltTag:    rebuiltTag,
			},
		}
		spinner.Increment()
		p.Wait()
		if !o.Options.IsTerminal() {
			o.Log.Info("Collected catalog %s", op.Catalog)
		}
	}

	o.Log.Debug(collectorPrefix+"related images length %d ", len(relatedImages))
	var count = 0
	if o.Options.LogLevel == "debug" {
		for _, v := range relatedImages {
			count += len(v)
		}
	}
	o.Log.Debug(collectorPrefix+"images to copy (before duplicates) %d ", count)
	var err error
	// check the mode
	switch {
	case o.Options.IsMirrorToDisk():
		allImages, err = oi.prepareM2DCopyBatch(relatedImages)
		if err != nil {
			o.Log.Error(errMsg, err.Error())
			return cs, fmt.Errorf("%w", err)
		}
	case o.Options.IsMirrorToMirror():
		allImages, err = oi.dispatchImagesForM2M(relatedImages)
		if err != nil {
			o.Log.Error(errMsg, err.Error())
			return cs, fmt.Errorf("%w", err)
		}
	case o.Options.IsDiskToMirror() || o.Options.IsDelete():
		allImages, err = oi.prepareD2MCopyBatch(relatedImages)
		if err != nil {
			o.Log.Error(errMsg, err.Error())
			return cs, fmt.Errorf("%w", err)
		}

	}

	collectorSchema.AllImages = allImages
	collectorSchema.CopyImageSchemaMap = *copyImageSchemaMap

	return collectorSchema, nil
}

func isFullCatalog(catalog v2alpha1.Operator) bool {
	return len(catalog.IncludeConfig.Packages) == 0 && catalog.Full
}

func createFolders(paths []string) error {
	var errs []error
	for _, path := range paths {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			err = os.MkdirAll(path, 0755)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func digestOfFilter(catalog v2alpha1.Operator) (string, error) {
	c := catalog
	c.TargetCatalog = ""
	c.TargetTag = ""
	c.TargetCatalogSourceTemplate = ""
	pkgs, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("%w", err)
	}
	// #nosec G401
	return fmt.Sprintf("%x", md5.Sum(pkgs))[0:32], nil
}

func (o Operator) isAlreadyFiltered(ctx context.Context, srcImage, filteredImageDigest string) bool {

	imgSpec, err := image.ParseRef(srcImage)
	if err != nil {
		o.Log.Debug(errMsg, err.Error())
		return false
	}

	sourceCtx := o.Options.NewSystemContext()
	// OCPBUGS-37948 : No TLS verification when getting manifests from the cache registry
	if strings.Contains(srcImage, o.Options.LocalStorageFQDN) { // when copying from cache, use HTTP
		sourceCtx.DockerInsecureSkipTLSVerify = types.OptionalBoolTrue
	}

	catalogDigest, err := manifest.GetDigest(ctx, sourceCtx, imgSpec.ReferenceWithTransport)
	if err != nil {
		o.Log.Debug(errMsg, err.Error())
		return false
	}
	return filteredImageDigest == catalogDigest
}

// isDeleteOfV1CatalogFromDisk returns true when trying to delete an operator catalog mirrored by oc-mirror v1 and the catalog was on disk (using oci:// on the ImageSetConfiguration)
// TODO remove me when the migration from oc-mirror v1 to v2 ends
func (o Operator) isDeleteOfV1CatalogFromDisk() bool {
	return o.Options.IsDiskToMirror() && o.Options.IsDelete() && o.Options.WithV1Tags
}

// TODO remove me when the migration from oc-mirror v1 to v2 ends
func addOriginFromOperatorCatalogOnDisk(relatedImages *map[string][]v2alpha1.RelatedImage) {
	for key, images := range *relatedImages {
		for i := range images {
			// Modify the RelatedImage object as needed
			images[i].OriginFromOperatorCatalogOnDisk = true
		}
		(*relatedImages)[key] = images
	}
}
