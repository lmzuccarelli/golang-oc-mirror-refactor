package operator

import (
	"context"
	"fmt"
	"strings"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/emoji"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/image"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/imagebuilder"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
)

type RebuildCatalogInterface interface {
	Rebuild(v2alpha1.CollectorSchema) error
}

type RebuildCatalog struct {
	Log     clog.PluggableLoggerInterface
	Options *common.MirrorOptions
	Config  v2alpha1.ImageSetConfiguration
	Context context.Context
}

func NewRebuildCatalog(ctx context.Context, log clog.PluggableLoggerInterface, cfg v2alpha1.ImageSetConfiguration, opts *common.MirrorOptions) RebuildCatalogInterface {
	return RebuildCatalog{
		Context: ctx,
		Log:     log,
		Options: opts,
		Config:  cfg,
	}
}

func (o RebuildCatalog) Rebuild(operatorImgs v2alpha1.CollectorSchema) error {
	// CLID-230 rebuild-catalogs
	oImgs := operatorImgs.AllImages
	if o.Options.IsMirrorToDisk() || o.Options.IsMirrorToMirror() {
		o.Log.Info(emoji.RepeatSingleButton + " rebuilding catalogs")

		for _, copyImage := range oImgs {

			if copyImage.Type == v2alpha1.TypeOperatorCatalog {
				if o.Options.IsMirrorToMirror() && strings.Contains(copyImage.Source, o.Options.LocalStorageFQDN) {
					// CLID-275: this is the ref to the already rebuilt catalog, which needs to be mirrored to destination.
					continue
				}
				ref, err := image.ParseRef(copyImage.Origin)
				if err != nil {
					return fmt.Errorf("unable to rebuild catalog %s: %v", copyImage.Origin, err)
				}
				filteredConfigPath := ""
				ctlgFilterResult, ok := operatorImgs.CatalogToFBCMap[ref.ReferenceWithTransport]
				if ok {
					filteredConfigPath = ctlgFilterResult.FilteredConfigPath
					if !ctlgFilterResult.ToRebuild {
						continue
					}
				} else {
					return fmt.Errorf("unable to rebuild catalog %s: filtered declarative config not found", copyImage.Origin)
				}
				builder := imagebuilder.NewGCRCatalogBuilder(o.Log, *o.Options)
				err = builder.RebuildCatalog(o.Context, copyImage, filteredConfigPath)
				if err != nil {
					return fmt.Errorf("unable to rebuild catalog %s: %v", copyImage.Origin, err)
				}
			}
		}
	}
	return nil
}
