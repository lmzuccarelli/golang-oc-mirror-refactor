package release

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/emoji"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/imagebuilder"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/manifest"
)

type GraphUpdateInterface interface {
	Create(string) (string, error)
}

type GraphUpdate struct {
	Log     clog.PluggableLoggerInterface
	Options *common.MirrorOptions
	Config  v2alpha1.ImageSetConfiguration
}

func NewGraphUpdate(log clog.PluggableLoggerInterface, cfg v2alpha1.ImageSetConfiguration, opts *common.MirrorOptions) GraphUpdate {
	return GraphUpdate{
		Log:     log,
		Config:  cfg,
		Options: opts,
	}
}

// createGraphImage creates a graph image from the graph data
// and returns the image reference.
// it follows https://docs.openshift.com/container-platform/4.13/updating/updating-restricted-network-cluster/restricted-network-update-osus.html#update-service-graph-data_updating-restricted-network-cluster-osus
func (o *GraphUpdate) Create(url string) (string, error) {
	if o.Config.Mirror.Platform.Graph {
		ctx := context.Background()
		image, err := o.graphImageInWorkingDir(ctx)

		// try download the graph data
		if len(image) == 0 && err != nil && o.Options.IsMirrorToDisk() {
			o.Log.Info(emoji.RepeatSingleButton + " building graph image")
			// HTTP Get the graph updates from api endpoint
			// #nosec G107
			// nolint: noctx
			resp, err := http.Get(url)
			if err != nil {
				return "", fmt.Errorf("%w", err)
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return "", fmt.Errorf("%w", err)
			}

			// save graph data in a container layer modifying UID and GID to root.
			archiveDestination := filepath.Join(o.Options.WorkingDir, graphArchive)
			graphLayer, err := imagebuilder.LayerFromGzipByteArray(body, archiveDestination, buildGraphDataDir, 0644, 0, 0)
			if err != nil {
				return "", fmt.Errorf("%w", err)
			}
			defer os.Remove(archiveDestination)

			// Create a local directory for saving the OCI image layout of UBI9
			layoutDir := filepath.Join(o.Options.WorkingDir, graphPreparationDir)
			if err := os.MkdirAll(layoutDir, os.ModePerm); err != nil {
				return "", fmt.Errorf("%w", err)
			}

			// Use the imgBuilder to pull the ubi9 image to layoutDir
			builder := imagebuilder.NewBuilder(o.Log, *o.Options)
			layoutPath, err := builder.SaveImageLayoutToDir(ctx, graphBaseImage, layoutDir)
			if err != nil {
				return "", fmt.Errorf("%w", err)
			}

			// preprare the CMD to []string{"/bin/bash", "-c", fmt.Sprintf("exec cp -rp %s/* %s", graphDataDir, graphDataMountPath)}
			cmd := []string{"/bin/bash", "-c", fmt.Sprintf("exec cp -rp %s/* %s", buildGraphDataDir, graphDataMountPath)}

			// update a ubi9 image with this new graphLayer and new cmd
			graphImageRef := filepath.Join(o.destinationRegistry(), graphImageName) + ":latest"
			_, err = builder.BuildAndPush(ctx, graphImageRef, layoutPath, cmd, graphLayer)
			if err != nil {
				return "", fmt.Errorf("%w", err)
			}
			return dockerProtocol + graphImageRef, nil
		}
		o.Log.Info("graph data exists in cache")
		return image, nil
	}
	return "", nil
}

func (o *GraphUpdate) graphImageInWorkingDir(ctx context.Context) (string, error) {
	var layoutDir string
	fullPath, _ := os.Getwd()
	if strings.Contains(fullPath, o.Options.WorkingDir) {
		layoutDir = filepath.Join(o.Options.WorkingDir, graphPreparationDir)
	} else {
		layoutDir = filepath.Join(fullPath[1:], o.Options.WorkingDir, graphPreparationDir)
	}
	graphImageRef := ociProtocol + layoutDir

	exists, err := o.imageExists(ctx, graphImageRef)
	if err != nil {
		return "", fmt.Errorf("no oci formatted graph image ready in cache: %w", err)
	}
	if !exists {
		return "", fmt.Errorf("no oci formatted graph image ready in cache")
	}
	return graphImageRef, nil
}

func (o *GraphUpdate) imageExists(ctx context.Context, ref string) (bool, error) {
	sourceCtx := o.Options.NewSystemContext()
	digest, err := manifest.GetDigest(ctx, sourceCtx, ref)
	if err != nil {
		return false, err
	}
	if digest == "" {
		return false, nil
	}
	return true, nil
}

func (o *GraphUpdate) destinationRegistry() string {
	if o.Options.DestinationRegistry == "" {
		if o.Options.IsDiskToMirror() || o.Options.IsMirrorToMirror() {
			o.Options.DestinationRegistry = strings.TrimPrefix(o.Options.Destination, dockerProtocol)
		} else {
			o.Options.DestinationRegistry = o.Options.LocalStorageFQDN
		}
	}
	return o.Options.DestinationRegistry
}
