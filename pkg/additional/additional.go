package additional

import (
	"fmt"
	"strings"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/image"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
)

const (
	dockerProtocol  = "docker://"
	ociProtocol     = "oci://"
	collectorPrefix = "[AdditionalImagesCollector] "
	errMsg          = collectorPrefix + "%s"
)

type CollectAdditional struct {
	Log     clog.PluggableLoggerInterface
	Options *common.MirrorOptions
	Config  v2alpha1.ImageSetConfiguration
}

func New(log clog.PluggableLoggerInterface, cfg v2alpha1.ImageSetConfiguration, opts *common.MirrorOptions) CollectAdditional {
	return CollectAdditional{
		Log:     log,
		Options: opts,
		Config:  cfg,
	}
}

func (o CollectAdditional) Collect() (v2alpha1.CollectorSchema, error) {

	allImages := []v2alpha1.CopyImageSchema{}
	cs := v2alpha1.CollectorSchema{}

	o.Log.Debug(collectorPrefix+"setting copy option MultiArch=%s when collecting releases image", o.Options.MultiArch)
	for _, img := range o.Config.ImageSetConfigurationSpec.Mirror.AdditionalImages {
		var src, dest, tmpSrc, tmpDest, origin string

		imgSpec, err := image.ParseRef(img.Name)
		if err != nil {
			// OCPBUGS-33081 - skip if parse error (i.e semver and other)
			o.Log.Warn("%v : SKIPPING", err)
			continue
		}
		// nolint: gocritic
		if o.Options.IsMirrorToDisk() || o.Options.IsMirrorToMirror() {
			tmpSrc = imgSpec.ReferenceWithTransport
			origin = img.Name
			if imgSpec.Transport == dockerProtocol {
				if imgSpec.IsImageByDigestOnly() {
					tmpDest = strings.Join([]string{o.Options.LocalStorageFQDN, imgSpec.PathComponent}, "/") + ":" + imgSpec.Algorithm + "-" + imgSpec.Digest
				} else if imgSpec.IsImageByTagAndDigest() {
					o.Log.Warn(collectorPrefix+"%s has both tag and digest : using digest to pull, but tag only for mirroring", imgSpec.Reference)
					tmpSrc = strings.Join([]string{imgSpec.Domain, imgSpec.PathComponent}, "/") + "@" + imgSpec.Algorithm + ":" + imgSpec.Digest
					tmpDest = strings.Join([]string{o.Options.LocalStorageFQDN, imgSpec.PathComponent}, "/") + ":" + imgSpec.Tag
				} else {
					tmpDest = strings.Join([]string{o.Options.LocalStorageFQDN, imgSpec.PathComponent}, "/") + ":" + imgSpec.Tag
				}
			} else {
				tmpDest = strings.Join([]string{o.Options.LocalStorageFQDN, strings.TrimPrefix(imgSpec.PathComponent, "/")}, "/") + ":latest"
			}

		} else if o.Options.IsDiskToMirror() {
			origin = img.Name
			imgSpec, err := image.ParseRef(img.Name)
			if err != nil {
				o.Log.Error(errMsg, err.Error())
				return cs, err
			}

			if imgSpec.Transport == dockerProtocol {
				if imgSpec.IsImageByDigestOnly() {
					tmpSrc = strings.Join([]string{o.Options.LocalStorageFQDN, imgSpec.PathComponent + ":" + imgSpec.Algorithm + "-" + imgSpec.Digest}, "/")
					if o.Options.GenerateV1DestTags {
						tmpDest = strings.Join([]string{o.Options.Destination, imgSpec.PathComponent + ":latest"}, "/")

					} else {
						tmpDest = strings.Join([]string{o.Options.Destination, imgSpec.PathComponent + ":" + imgSpec.Algorithm + "-" + imgSpec.Digest}, "/")
					}
				} else if imgSpec.IsImageByTagAndDigest() {
					// OCPBUGS-33196 + OCPBUGS-37867- check source image for tag and digest
					// use tag only for both src and dest
					o.Log.Warn(collectorPrefix+"%s has both tag and digest : using tag only", imgSpec.Reference)
					tmpSrc = strings.Join([]string{o.Options.LocalStorageFQDN, imgSpec.PathComponent}, "/") + ":" + imgSpec.Tag
					tmpDest = strings.Join([]string{o.Options.Destination, imgSpec.PathComponent}, "/") + ":" + imgSpec.Tag
				} else {
					tmpSrc = strings.Join([]string{o.Options.LocalStorageFQDN, imgSpec.PathComponent}, "/") + ":" + imgSpec.Tag
					tmpDest = strings.Join([]string{o.Options.Destination, imgSpec.PathComponent}, "/") + ":" + imgSpec.Tag
				}

			} else {
				tmpSrc = strings.Join([]string{o.Options.LocalStorageFQDN, strings.TrimPrefix(imgSpec.PathComponent, "/")}, "/") + ":latest"
				tmpDest = strings.Join([]string{o.Options.Destination, strings.TrimPrefix(imgSpec.PathComponent, "/")}, "/") + ":latest"
			}

		}
		if tmpSrc == "" || tmpDest == "" {
			return cs, fmt.Errorf(collectorPrefix+"unable to determine src %s or dst %s for %s", tmpSrc, tmpDest, img.Name)
		}
		srcSpec, err := image.ParseRef(tmpSrc) // makes sure this ref is valid, and adds transport if needed
		if err != nil {
			return cs, err
		}
		src = srcSpec.ReferenceWithTransport

		destSpec, err := image.ParseRef(tmpDest) // makes sure this ref is valid, and adds transport if needed
		if err != nil {
			return cs, err
		}
		dest = destSpec.ReferenceWithTransport

		o.Log.Debug(collectorPrefix+"source %s", src)
		o.Log.Debug(collectorPrefix+"destination %s", dest)

		allImages = append(allImages, v2alpha1.CopyImageSchema{Source: src, Destination: dest, Origin: origin, Type: v2alpha1.TypeGeneric})
	}
	cs.AllImages = allImages
	return cs, nil
}
