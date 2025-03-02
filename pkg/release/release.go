package release

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/image"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/manifest"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/mirror"
	digest "github.com/opencontainers/go-digest"
	"gopkg.in/yaml.v2"
)

const (
	graphBaseImage                 = "registry.access.redhat.com/ubi9/ubi:latest"
	graphURL                       = "https://api.openshift.com/api/upgrades_info/graph-data"
	graphArchive                   = "cincinnati-graph-data.tar"
	graphPreparationDir            = "graph-preparation"
	buildGraphDataDir              = "/var/lib/cincinnati-graph-data"
	graphDataMountPath             = "/var/lib/cincinnati/graph-data"
	graphImageName                 = "openshift/graph-image"
	indexJson                      = "manifest.json"
	workingDir                     = "working-dir"
	dockerProtocol                 = "docker://"
	ociProtocol                    = "oci://"
	ociProtocolTrimmed             = "oci:"
	dirProtocol                    = "dir://"
	dirProtocolTrimmed             = "dir:"
	releaseImageDir                = "release-images"
	releaseIndex                   = "release-index"
	cincinnatiGraphDataDir         = "cincinnati-graph-data"
	releaseImageExtractDir         = "hold-release"
	releaseManifests               = "release-manifests"
	releaseBootableImages          = "0000_50_installer_coreos-bootimages.yaml"
	releaseBootableImagesFullPath  = releaseManifests + "/" + releaseBootableImages
	imageReferences                = "image-references"
	releaseImageExtractFullPath    = releaseManifests + "/" + imageReferences
	blobsDir                       = "blobs/sha256"
	collectorPrefix                = "[ReleaseImageCollector] "
	errMsg                         = collectorPrefix + "%s"
	logFile                        = "release.log"
	releaseImagePathComponents     = "openshift/release-images"
	releaseComponentPathComponents = "openshift/release"
)

type CollectRelease struct {
	Log     clog.PluggableLoggerInterface
	Options *common.MirrorOptions
	Config  v2alpha1.ImageSetConfiguration
	Context context.Context
	Mirror  mirror.MirrorInterface
}

func New(ctx context.Context, log clog.PluggableLoggerInterface, mi mirror.MirrorInterface, cfg v2alpha1.ImageSetConfiguration, opts *common.MirrorOptions) common.ImageCollectorInteface {
	return CollectRelease{
		Context: ctx,
		Log:     log,
		Options: opts,
		Config:  cfg,
		Mirror:  mi,
	}
}

func (o CollectRelease) Collect() (v2alpha1.CollectorSchema, error) {
	// we just care for 1 platform release, in order to read release images
	o.Log.Debug(collectorPrefix+"setting copy option o.Opts.MultiArch=%s when collecting releases image", o.Options.MultiArch)
	var allImages []v2alpha1.CopyImageSchema
	var imageIndexDir string
	cs := v2alpha1.CollectorSchema{}
	if o.Options.IsMirrorToDisk() || o.Options.IsMirrorToMirror() {
		for _, img := range o.Config.Mirror.Platform.Releases {
			hld := strings.Split(img.Name, "/")
			releaseRepoAndTag := hld[len(hld)-1]
			imageIndexDir = strings.Replace(releaseRepoAndTag, ":", "/", -1)
			releaseTag := releaseRepoAndTag[strings.Index(releaseRepoAndTag, ":")+1:]
			cacheDir := filepath.Join(o.Options.WorkingDir, releaseImageExtractDir, imageIndexDir)
			dir := filepath.Join(o.Options.WorkingDir, releaseImageDir, imageIndexDir)
			imgSpec, err := image.ParseRef(img.Name)
			if err != nil {
				return cs, fmt.Errorf(errMsg, err.Error())
			}

			src := dockerProtocol + imgSpec.Reference
			dest := ociProtocolTrimmed + dir

			if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
				o.Log.Debug(collectorPrefix+"copying  release image %s ", img)
				err := os.MkdirAll(dir, 0755)
				if err != nil {
					return cs, fmt.Errorf(errMsg, err.Error())
				}

				err = o.Mirror.Copy(o.Context, src, dest, o.Options)

				if err != nil {
					return cs, fmt.Errorf(errMsg, err.Error())
				}
				o.Log.Debug(collectorPrefix+"copied release index image %s ", img)
			} else {
				o.Log.Debug(collectorPrefix+"release-images index directory alredy exists %s", dir)
			}

			oci, err := manifest.GetImageIndex(dir)
			if err != nil {
				return cs, fmt.Errorf(errMsg, err.Error())
			}

			//read the link to the manifest
			if len(oci.Manifests) == 0 {
				return cs, fmt.Errorf(errMsg, "image index not found ")
			}
			validDigest, err := digest.Parse(oci.Manifests[0].Digest)
			if err != nil {
				return cs, fmt.Errorf(collectorPrefix+"invalid digest for image index %s: %s", oci.Manifests[0].Digest, err.Error())
			}

			mfst := validDigest.Encoded()
			o.Log.Debug(collectorPrefix+"image manifest digest %s", mfst)

			manifestDir := filepath.Join(dir, blobsDir, mfst)
			m, err := manifest.GetImageManifest(manifestDir)
			if err != nil {
				return cs, fmt.Errorf(errMsg, err.Error())
			}
			o.Log.Debug(collectorPrefix+"config digest %s ", oci.Config.Digest)

			fromDir := strings.Join([]string{dir, blobsDir}, "/")
			err = manifest.ExtractLayersOCI(fromDir, cacheDir, releaseManifests, m)
			if err != nil {
				return cs, fmt.Errorf(errMsg, err.Error())
			}
			o.Log.Debug("extracted layer %s ", cacheDir)

			// overkill but its used for consistency
			releaseDir := strings.Join([]string{cacheDir, releaseImageExtractFullPath}, "/")
			allRelatedImages, err := manifest.GetReleaseSchema(releaseDir)
			if err != nil {
				return cs, fmt.Errorf(errMsg, err.Error())
			}

			if o.Config.Mirror.Platform.KubeVirtContainer {
				ki, err := getKubeVirtImage(cacheDir)
				if err != nil {
					o.Log.Warn("%v", err)
				} else {
					allRelatedImages = append(allRelatedImages, ki)
				}
			}

			//add the release image itself
			allRelatedImages = append(allRelatedImages, v2alpha1.RelatedImage{Image: img.Name, Name: img.Name, Type: v2alpha1.TypeOCPRelease})
			tmpAllImages, err := prepareM2DCopyBatch(allRelatedImages, o.Options, releaseTag)
			if err != nil {
				return cs, err
			}
			allImages = append(allImages, tmpAllImages...)
		}

		if o.Config.Mirror.Platform.Graph {
			graphImage, err := handleGraphImage(o.Context, o.Options)
			if err != nil {
				o.Log.Warn("could not process graph image - SKIPPING: %v", err)
			} else if graphImage.Source != "" {
				allImages = append(allImages, graphImage)
			}
		}
	} else if o.Options.IsDiskToMirror() {
		releaseFolders := []string{}
		for _, releaseImg := range o.Config.Mirror.Platform.Releases {
			releaseRef, err := image.ParseRef(releaseImg.Name)
			if err != nil {
				return cs, fmt.Errorf(errMsg, err.Error())
			}
			if releaseRef.Tag == "" && len(releaseRef.Digest) == 0 {
				return cs, fmt.Errorf(errMsg, "release image "+releaseImg.Name+" doesn't have a tag or digest")
			}
			tag := releaseRef.Tag
			if releaseRef.Tag == "" && len(releaseRef.Digest) > 0 {
				tag = releaseRef.Digest
			}
			relatedImage := v2alpha1.RelatedImage{
				Image: releaseImg.Name,
				Type:  v2alpha1.TypeOCPRelease,
			}
			monoReleaseSlice, err := prepareD2MCopyBatch([]v2alpha1.RelatedImage{relatedImage}, o.Options, tag)
			if err != nil {
				return cs, fmt.Errorf(errMsg, err.Error())
			}
			allImages = append(allImages, monoReleaseSlice...)
			pathComponent := path.Base(releaseRef.PathComponent) + "@" + releaseRef.Algorithm
			releaseMetaDataPath := path.Join(o.Options.WorkingDir, releaseImageExtractDir, pathComponent, tag)
			releaseFolders = append(releaseFolders, releaseMetaDataPath)
		}

		for _, releaseDir := range releaseFolders {

			releaseTag := filepath.Base(releaseDir)

			// get all release images from manifest (json)
			imageReferencesFile := filepath.Join(releaseDir, releaseManifests, imageReferences)
			releaseRelatedImages, err := manifest.GetReleaseSchema(imageReferencesFile)
			if err != nil {
				return cs, fmt.Errorf(errMsg, err.Error())
			}

			if o.Config.Mirror.Platform.KubeVirtContainer {
				cacheDir := filepath.Join(releaseDir)
				ki, err := getKubeVirtImage(cacheDir)
				if err != nil {
					o.Log.Warn("%v", err)
				} else {
					releaseRelatedImages = append(releaseRelatedImages, ki)
				}
			}

			releaseCopyImages, err := prepareD2MCopyBatch(releaseRelatedImages, o.Options, releaseTag)
			if err != nil {
				return cs, err
			}
			allImages = append(allImages, releaseCopyImages...)
		}

		if o.Config.Mirror.Platform.Graph {
			o.Log.Debug("adding graph data image")
			graphRelatedImage := v2alpha1.RelatedImage{
				Name: graphImageName,
				// Supposing that the mirror to disk saved the image with the latest tag
				// If this supposition is false, then we need to implement a mechanism to save
				// the digest of the graph image and use it here
				Image: dockerProtocol + filepath.Join(o.Options.LocalStorageFQDN, graphImageName) + ":latest",
				Type:  v2alpha1.TypeCincinnatiGraph,
			}
			// OCPBUGS-38037: Check the graph image is in the cache before adding it
			// graphInCache, err := imageExists(ctx, graphRelatedImage.Image)
			// OCPBUGS-43825: The check graphInCache is relevant for DiskToMirror workflow only, not for delete workflow
			// In delete workflow, the graph image might have been mirrored with M2M, and the graph image might have
			// therefore been pushed directly to the destination registry. It will not exist in the cache, and that should be ok.
			// Nevertheless, in DiskToMirror, and as explained in OCPBUGS-38037, the graphInCache check is important
			// because in enclave environment, the Cincinnati API may not have been called, so we rely on the existance of the
			// graph image in the cache as a paliative.
			// shouldProceed := graphInCache || opts.Mode == "delete"
			// if err != nil && o.Options.Mode != "delete" {
			//	o.Log.Warn("unable to find graph image in local cache: %v. SKIPPING", err)
			// }
			// if shouldProceed {
			// OCPBUGS-26513: In order to get the destination for the graphDataImage
			// into `o.GraphDataImage`, we call `prepareD2MCopyBatch` on an array
			// containing only the graph image. This way we can easily identify the destination
			// of the graph image.
			graphImageSlice := []v2alpha1.RelatedImage{graphRelatedImage}
			graphCopySlice, err := prepareD2MCopyBatch(graphImageSlice, o.Options, "")
			if err != nil {
				return cs, err
			}
			// if there is no error, we are certain that the slice only contains 1 element
			// but double checking...
			if len(graphCopySlice) != 1 {
				return cs, fmt.Errorf(collectorPrefix + "error while calculating the destination reference for the graph image")
			}
			//o.GraphDataImage = graphCopySlice[0].Destination
			allImages = append(allImages, graphCopySlice...)
			//}
		}
	}

	//OCPBUGS-43275: deduplicating
	slices.SortFunc(allImages, func(a, b v2alpha1.CopyImageSchema) int {
		cmp := strings.Compare(a.Origin, b.Origin)
		if cmp == 0 {
			cmp = strings.Compare(a.Source, b.Source)
		}
		if cmp == 0 {
			// this comparison is important because the same digest can be used
			// several times in image-references for different components
			cmp = strings.Compare(a.Destination, b.Destination)
		}
		return cmp
	})
	allImages = slices.Compact(allImages)
	cs.AllImages = allImages
	return cs, nil
}

func prepareM2DCopyBatch(images []v2alpha1.RelatedImage, opts *common.MirrorOptions, releaseTag string) ([]v2alpha1.CopyImageSchema, error) {
	var result []v2alpha1.CopyImageSchema
	for _, img := range images {
		var src string
		var dest string

		imgSpec, err := image.ParseRef(img.Image)
		if err != nil {
			return nil, err
		}
		src = imgSpec.ReferenceWithTransport

		pathComponents := preparePathComponents(imgSpec, img.Type, img.Name)
		tag := prepareTag(imgSpec, img.Type, releaseTag, img.Name)

		dest = dockerProtocol + strings.Join([]string{opts.LocalStorageFQDN, pathComponents + ":" + tag}, "/")

		result = append(result, v2alpha1.CopyImageSchema{Origin: img.Image, Source: src, Destination: dest, Type: img.Type})
	}
	return result, nil
}

func prepareD2MCopyBatch(images []v2alpha1.RelatedImage, opts *common.MirrorOptions, releaseTag string) ([]v2alpha1.CopyImageSchema, error) {
	var result []v2alpha1.CopyImageSchema
	for _, img := range images {
		var src string
		var dest string

		imgSpec, err := image.ParseRef(img.Image)
		if err != nil {
			return nil, err
		}

		pathComponents := preparePathComponents(imgSpec, img.Type, img.Name)
		tag := prepareTag(imgSpec, img.Type, releaseTag, img.Name)

		src = dockerProtocol + strings.Join([]string{opts.LocalStorageFQDN, pathComponents + ":" + tag}, "/")
		dest = strings.Join([]string{opts.Destination, pathComponents + ":" + tag}, "/")

		if src == "" || dest == "" {
			return result, fmt.Errorf("unable to determine src %s or dst %s for %s", src, dest, img.Name)
		}
		result = append(result, v2alpha1.CopyImageSchema{Origin: img.Image, Source: src, Destination: dest, Type: img.Type})
	}
	return result, nil
}

// assumes this is called during DiskToMirror workflow.
// this method doesn't verify if the graphImage has been generated
// by the collector.
func GraphImage(opts common.MirrorOptions) (string, error) {
	if opts.GraphImage == "" {
		sourceGraphDataImage := filepath.Join(opts.LocalStorageFQDN, graphImageName) + ":latest"
		graphRelatedImage := []v2alpha1.RelatedImage{
			{
				Name:  "release",
				Image: sourceGraphDataImage,
				Type:  v2alpha1.TypeCincinnatiGraph,
			},
		}
		graphCopyImage, err := prepareD2MCopyBatch(graphRelatedImage, &opts, "")
		if err != nil {
			return "", fmt.Errorf("[release collector] could not establish the destination for the graph image: %v", err)
		}
		opts.GraphImage = graphCopyImage[0].Destination
	}
	return opts.GraphImage, nil
}

// getKubeVirtImage - CLID-179 : include coreos-bootable container image
// if set it will be across the board for all releases
func getKubeVirtImage(releaseArtifactsDir string) (v2alpha1.RelatedImage, error) {
	var ibi v2alpha1.InstallerBootableImages
	var icm v2alpha1.InstallerConfigMap

	// parse the main yaml file
	biFile := strings.Join([]string{releaseArtifactsDir, releaseBootableImagesFullPath}, "/")
	file, err := os.ReadFile(biFile)
	if err != nil {
		return v2alpha1.RelatedImage{}, fmt.Errorf("reading kubevirt yaml file %v", err)
	}

	errs := yaml.Unmarshal(file, &icm)
	if errs != nil {
		// this should not break the release process
		// we just report the error and continue
		return v2alpha1.RelatedImage{}, fmt.Errorf("marshalling kubevirt yaml file %v", errs)
	}

	// now parse the json section
	errs = json.Unmarshal([]byte(icm.Data.Stream), &ibi)
	if errs != nil {
		// this should not break the release process
		// we just report the error and continue
		return v2alpha1.RelatedImage{}, fmt.Errorf("parsing json from kubevirt configmap data %v", errs)
	}

	image := ibi.Architectures.X86_64.Images.Kubevirt.DigestRef
	if image == "" {
		return v2alpha1.RelatedImage{}, fmt.Errorf("could not find kubevirt image in this release")
	}
	kubeVirtImage := v2alpha1.RelatedImage{
		Image: image,
		Name:  "kube-virt-container",
		Type:  v2alpha1.TypeOCPReleaseContent,
	}
	return kubeVirtImage, nil
}

func handleGraphImage(ctx context.Context, opts *common.MirrorOptions) (v2alpha1.CopyImageSchema, error) {
	if updateURLOverride := os.Getenv("UPDATE_URL_OVERRIDE"); len(updateURLOverride) != 0 {

		// OCPBUGS-38037: this indicates that the official cincinnati API is not reacheable
		// and that graph image cannot be rebuilt on top the complete graph in tar.gz format
		graphImgRef := dockerProtocol + filepath.Join(opts.DestinationRegistry, graphImageName) + ":latest"

		cachedImageRef := dockerProtocol + filepath.Join(opts.LocalStorageFQDN, graphImageName) + ":latest"
		graphCopy := v2alpha1.CopyImageSchema{
			Source:      cachedImageRef,
			Destination: graphImgRef,
			Origin:      cachedImageRef,
			Type:        v2alpha1.TypeCincinnatiGraph,
		}
		return graphCopy, nil
	} else {
		return v2alpha1.CopyImageSchema{}, nil
	}
}

func preparePathComponents(imgSpec image.ImageSpec, imgType v2alpha1.ImageType, imgName string) string {
	pathComponents := ""
	switch {
	case imgType == v2alpha1.TypeOCPRelease:
		pathComponents = releaseImagePathComponents
	case imgType == v2alpha1.TypeCincinnatiGraph:
		pathComponents = imgSpec.PathComponent
	case imgType == v2alpha1.TypeOCPReleaseContent && imgName != "":
		pathComponents = releaseComponentPathComponents
	case imgSpec.IsImageByDigestOnly():
		pathComponents = imgSpec.PathComponent
	}

	return pathComponents
}

func prepareTag(imgSpec image.ImageSpec, imgType v2alpha1.ImageType, releaseTag, imgName string) string {
	tag := imgSpec.Tag

	switch {
	case imgType == v2alpha1.TypeOCPRelease || imgType == v2alpha1.TypeCincinnatiGraph:
		// OCPBUGS-44033 mirroring release with no release tag
		// i.e by digest registry.ci.openshift.org/ocp/release@sha256:0fb444ec9bb1b01f06dd387519f0fe5b4168e2d09a015697a26534fc1565c5e7
		if len(imgSpec.Tag) == 0 {
			tag = releaseTag
		} else {
			tag = imgSpec.Tag
		}
	case imgType == v2alpha1.TypeOCPReleaseContent && imgName != "":
		tag = releaseTag + "-" + imgName
	case imgSpec.IsImageByDigestOnly():
		tag = fmt.Sprintf("%s-%s", imgSpec.Algorithm, imgSpec.Digest)
		if len(tag) > 128 {
			tag = tag[:127]
		}
	}

	return tag

}
