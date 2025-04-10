package delete

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/otiai10/copy"
	"sigs.k8s.io/yaml"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/archive"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/batch"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/emoji"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/image"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
)

// TODO:
// refactor to a global const in one pkg
const (
	deleteDir                   string = "/delete"
	deleteImagesYaml            string = "delete/delete-images.yaml"
	discYaml                    string = "delete/delete-imageset-config.yaml"
	dockerProtocol              string = "docker://"
	operatorImageExtractDir     string = "hold-operator"
	ociProtocol                 string = "oci://"
	ociProtocolTrimmed          string = "oci:"
	operatorImageDir            string = "operator-images"
	blobsDir                    string = "docker/registry/v2/blobs/sha256"
	releaseManifests            string = "release-manifests"
	imageReferences             string = "image-references"
	deleteImagesErrMsg          string = "[delete-images] %v"
	releaseImageExtractFullPath string = releaseManifests + "/" + imageReferences
	releaseImageExtractDir      string = "hold-release"
	ocpRelease                  string = "ocp-release"
	errMsg                      string = "[ReleaseImageCollector] %v "
	logFile                     string = "release.log"
	x86_64                      string = "x86_64"
	amd64                       string = "x86_64"
	s390x                       string = "s390x"
	ppc64le                     string = "ppc64le"
	aarch64                     string = "aarch64"
	arm64                       string = "aarch64"
	multi                       string = "multi"
	releaseRepo                 string = "docker://quay.io/openshift-release-dev/ocp-release"
)

type DeleteInterface interface {
	WriteDeleteMetaData([]v2alpha1.CopyImageSchema) error
	ReadDeleteMetaData() (v2alpha1.DeleteImageList, error)
	DeleteRegistryImages(images v2alpha1.DeleteImageList) error
}

type DeleteImages struct {
	Log     clog.PluggableLoggerInterface
	Options *common.MirrorOptions
	Batch   batch.BatchInterface
	Blobs   archive.BlobsGatherer
	Config  v2alpha1.DeleteImageSetConfiguration
}

func New(
	log clog.PluggableLoggerInterface,
	opts *common.MirrorOptions,
	batch batch.BatchInterface,
	blobs archive.BlobsGatherer,
	config v2alpha1.DeleteImageSetConfiguration,
) *DeleteImages {
	return &DeleteImages{
		Log:     log,
		Options: opts,
		Batch:   batch,
		Blobs:   blobs,
		Config:  config,
	}
}

// WriteDeleteMetaData
func (o DeleteImages) WriteDeleteMetaData(images []v2alpha1.CopyImageSchema) error {
	o.Log.Info(emoji.PageFacingUp + " Generating delete file...")
	o.Log.Info("%s file created", o.Options.WorkingDir+deleteDir)

	// we write the image and related blobs in yaml format to file for further processing
	filename := filepath.Join(o.Options.WorkingDir, deleteImagesYaml)
	discYamlFile := filepath.Join(o.Options.WorkingDir, discYaml)
	// used for versioning and comparing
	if len(o.Options.DeleteID) > 0 {
		filename = filepath.Join(o.Options.WorkingDir, strings.ReplaceAll(deleteImagesYaml, ".", "-"+o.Options.DeleteID+"."))
		discYamlFile = filepath.Join(o.Options.WorkingDir, strings.ReplaceAll(discYaml, ".", "-"+o.Options.DeleteID+"."))
	}
	// create the delete folder
	err := os.MkdirAll(o.Options.WorkingDir+deleteDir, 0755)
	if err != nil {
		o.Log.Error("%v ", err)
	}

	duplicates := []string{}
	var items []v2alpha1.DeleteItem
	for _, img := range images {
		if slices.Contains(duplicates, img.Origin) {
			o.Log.Debug("duplicate image found %s", img.Origin)
		} else {
			duplicates = append(duplicates, img.Origin)
			item := v2alpha1.DeleteItem{
				ImageName:      img.Origin,
				ImageReference: img.Destination,
				Type:           img.Type,
			}
			items = append(items, item)
		}
	}

	// sort the items
	sort.SliceStable(items, func(i, j int) bool {
		return items[i].ImageReference < items[j].ImageReference
	})
	// marshal to yaml and write to file
	deleteImageList := v2alpha1.DeleteImageList{
		Kind:       "DeleteImageList",
		APIVersion: "mirror.openshift.io/v2alpha1",
		Items:      items,
	}
	ymlData, err := yaml.Marshal(deleteImageList)
	if err != nil {
		o.Log.Error(deleteImagesErrMsg, err)
	}
	// #nosec G306
	err = os.WriteFile(filename, ymlData, 0755)
	if err != nil {
		o.Log.Error(deleteImagesErrMsg, err)
	}

	err = copy.Copy(o.Options.ConfigPath, discYamlFile)
	if err != nil {
		o.Log.Error(deleteImagesErrMsg, err)
	}
	return nil
}

// DeleteRegistryImages - deletes both remote and local registries
func (o DeleteImages) DeleteRegistryImages(deleteImageList v2alpha1.DeleteImageList) error {
	o.Log.Debug("deleting images from remote registry")
	collectorSchema := v2alpha1.CollectorSchema{AllImages: []v2alpha1.CopyImageSchema{}}

	var batchError error

	increment := 1
	if o.Options.ForceCacheDelete {
		increment = 2
	}

	for _, img := range deleteImageList.Items {
		// OCPBUGS-43489
		// Verify that the "delete" destination is set correctly
		// It does not hurt to check each entry :)
		// This will avoid the error "Image may not exist or is not stored with a v2 Schema in a v2 registry"
		// Reverts OCPBUGS-44448
		imgSpecName, err := image.ParseRef(img.ImageName)
		if err != nil {
			return err
		}
		imgSpecRef, err := image.ParseRef(img.ImageReference)
		if err != nil {
			return err
		}
		// remove dockerProtocol
		name := strings.Split(o.Options.DeleteDestination, dockerProtocol)
		// this should not occur - but just incase
		if len(name) < 2 {
			return fmt.Errorf("delete destination is not well formed (%s) - missing dockerProtocol?", o.Options.DeleteDestination)
		}
		assembleName := name[1] + "/" + imgSpecName.PathComponent
		// check image type for release or release content
		//nolint: exhaustive
		switch img.Type {
		case v2alpha1.TypeOCPReleaseContent:
			assembleName = name[1] + "/openshift/release"
		case v2alpha1.TypeOCPRelease:
			assembleName = name[1] + "/openshift/release-images"
		}
		// check the assembled name against the reference name
		if assembleName != imgSpecRef.Name {
			return fmt.Errorf("delete destination %s does not match values found in the delete-images yaml file (please verify full name)", o.Options.DeleteDestination)
		}
		cis := v2alpha1.CopyImageSchema{
			Origin:      img.ImageName,
			Destination: img.ImageReference,
			Type:        img.Type,
		}
		o.Log.Debug("deleting images %v", cis.Destination)
		collectorSchema.AllImages = append(collectorSchema.AllImages, cis)

		if o.Options.ForceCacheDelete {
			cis := v2alpha1.CopyImageSchema{
				Origin:      img.ImageName,
				Destination: strings.ReplaceAll(img.ImageReference, o.Options.DeleteDestination, dockerProtocol+o.Options.LocalStorageFQDN),
				Type:        img.Type,
			}
			o.Log.Debug("deleting images local cache %v", cis.Destination)
			collectorSchema.AllImages = append(collectorSchema.AllImages, cis)
		}

		switch {
		case img.Type.IsRelease():
			collectorSchema.TotalReleaseImages += increment
		case img.Type.IsOperator():
			collectorSchema.TotalOperatorImages += increment
		case img.Type.IsAdditionalImage():
			collectorSchema.TotalAdditionalImages += increment
		case img.Type.IsHelmImage():
			collectorSchema.TotalHelmImages += increment
		}
	}

	o.Options.Stdout = io.Discard
	if !o.Options.DeleteGenerate && len(o.Options.DeleteDestination) > 0 {
		if _, err := o.Batch.Worker(context.Background(), collectorSchema, o.Options); err != nil {
			return fmt.Errorf("%w", err)
		}
	}

	if batchError != nil {
		o.Log.Warn("error during registry deletion: %v", batchError)
	}
	return nil
}

// ReadDeleteMetaData - read the list of images to delete
// used to verify the delete yaml is well formed as well as being
// the base for both local cache delete and remote registry delete
func (o DeleteImages) ReadDeleteMetaData() (v2alpha1.DeleteImageList, error) {
	o.Log.Info(emoji.Eyes + " Reading delete file...")
	var list v2alpha1.DeleteImageList
	var fileName string

	if len(o.Options.DeleteYaml) == 0 {
		fileName = filepath.Join(o.Options.WorkingDir, deleteImagesYaml)
		if _, err := os.Stat(fileName); os.IsNotExist(err) {
			return list, fmt.Errorf("delete yaml file %s does not exist (please perform a delete with --dry-run)", fileName)
		}
	} else {
		fileName = o.Options.DeleteYaml
	}

	data, err := os.ReadFile(fileName)
	if err != nil {
		return list, fmt.Errorf("%w", err)
	}
	// lets parse the file to get the images
	err = yaml.Unmarshal(data, &list)
	if err != nil {
		return list, fmt.Errorf("%w", err)
	}
	return list, nil
}
