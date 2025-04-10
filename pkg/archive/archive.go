package archive

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/history"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
	digest "github.com/opencontainers/go-digest"
)

const (
	archiveFilePrefix           = "mirror"
	imageSetConfigPrefix        = "isc_"
	cacheRepositoriesDir        = "docker/registry/v2/repositories"
	cacheBlobsDir               = "docker/registry/v2/blobs"
	cacheFilePrefix             = "docker/registry/v2"
	workingDirectory            = "working-dir"
	errMessageFolder            = "unable to create folder %s: %w"
	segMultiplier         int64 = 1024 * 1024 * 1024
	defaultSegSize        int64 = 500
	archiveFileNameFormat       = "%s_%06d.tar"
)

type BlobsGatherer interface {
	GatherBlobs(ctx context.Context, imgRef string) (map[string]string, error)
}

type Archiver interface {
	BuildArchive(ctx context.Context, collectedImages []v2alpha1.CopyImageSchema) error
}

type UnArchiver interface {
	Unarchive() error
}

type archiveAdder interface {
	addFile(pathToFile string, pathInTar string) error
	addAllFolder(folderToAdd string, relativeTo string) error
	close() error
}

type MirrorArchive struct {
	Archiver
	adder        archiveAdder
	destination  string
	iscPath      string
	workingDir   string
	cacheDir     string
	history      history.History
	blobGatherer BlobsGatherer
}

// NewMirrorArchive creates a new MirrorArchive instance with permissiveAdder:
// any files that exceed the maxArchiveSize specified in the imageSetConfig will
// be added to standalone archives, and flagged in a warning at the end of the execution
func NewPermissiveMirrorArchive(opts *common.MirrorOptions, log clog.PluggableLoggerInterface, maxSize int64) (*MirrorArchive, error) {

	// create the history interface
	history, err := history.NewHistory(opts.WorkingDir, opts.Since, log, history.OSFileCreator{})
	if err != nil {
		return &MirrorArchive{}, fmt.Errorf("%w", err)
	}

	bg := NewImageBlobGatherer(opts)

	if maxSize == 0 {
		maxSize = defaultSegSize
	}
	maxSize *= segMultiplier

	a, err := newPermissiveAdder(maxSize, opts.Destination, log)
	if err != nil {
		return &MirrorArchive{}, fmt.Errorf("%w", err)
	}

	ma := MirrorArchive{
		destination:  opts.Destination,
		history:      history,
		blobGatherer: bg,
		workingDir:   opts.WorkingDir,
		cacheDir:     opts.CacheDir,
		iscPath:      opts.ConfigPath,
		adder:        a,
	}
	return &ma, nil
}

// BuildArchive creates an archive that contains:
// * docker/v2/repositories : manifests for all mirrored images
// * docker/v2/blobs/sha256 : blobs that haven't been mirrored (diff)
// * working-dir
// * image set config
func (o *MirrorArchive) BuildArchive(ctx context.Context, collectedImages []v2alpha1.CopyImageSchema) error {
	// 0 - make sure that any tarWriters or files opened by the adder are closed as we leave this method
	defer o.adder.close()
	// 1 - Add files and directories under the cache's docker/v2/repositories to the archive
	repositoriesDir := filepath.Join(o.cacheDir, cacheRepositoriesDir)
	err := o.adder.addAllFolder(repositoriesDir, o.cacheDir)
	if err != nil {
		return fmt.Errorf("unable to add cache repositories to the archive : %w", err)
	}
	// 2- Add working-dir contents to archive
	err = o.adder.addAllFolder(o.workingDir, filepath.Dir(o.workingDir))
	if err != nil {
		return fmt.Errorf("unable to add working-dir to the archive : %w", err)
	}
	// 3 - Add imageSetConfig
	iscName := imageSetConfigPrefix + time.Now().UTC().Format(time.RFC3339)
	err = o.adder.addFile(o.iscPath, iscName)
	if err != nil {
		return fmt.Errorf("unable to add image set configuration to the archive : %w", err)
	}
	// 4 - Add blobs
	blobsInHistory, err := o.history.Read()
	if err != nil && !errors.Is(err, &history.EmptyHistoryError{}) {
		return fmt.Errorf("unable to read history metadata from working-dir : %w", err)
	}
	// ignoring the error otherwise: continuing with an empty map in blobsInHistory

	addedBlobs, err := o.addImagesDiff(ctx, collectedImages, blobsInHistory, o.cacheDir)
	if err != nil {
		return fmt.Errorf("unable to add image blobs to the archive : %w", err)
	}
	// 5 - update history file with addedBlobs
	_, err = o.history.Append(addedBlobs)
	if err != nil {
		return fmt.Errorf("unable to update history metadata: %w", err)
	}

	return nil
}

func (o *MirrorArchive) addImagesDiff(ctx context.Context, collectedImages []v2alpha1.CopyImageSchema, historyBlobs map[string]string, cacheDir string) (map[string]string, error) {
	allAddedBlobs := map[string]string{}
	for _, img := range collectedImages {
		imgBlobs, err := o.blobGatherer.GatherBlobs(ctx, img.Destination)
		if err != nil {
			return nil, fmt.Errorf("unable to find blobs corresponding to %s: %w", img.Destination, err)
		}

		addedBlobs, err := o.addBlobsDiff(imgBlobs, historyBlobs, allAddedBlobs)
		if err != nil {
			return nil, fmt.Errorf("unable to add blobs corresponding to %s: %w", img.Destination, err)
		}

		for hash, value := range addedBlobs {
			allAddedBlobs[hash] = value
		}

	}

	return allAddedBlobs, nil
}

func (o *MirrorArchive) addBlobsDiff(collectedBlobs, historyBlobs map[string]string, alreadyAddedBlobs map[string]string) (map[string]string, error) {
	blobsInDiff := map[string]string{}
	for hash := range collectedBlobs {
		_, alreadyMirrored := historyBlobs[hash]
		_, previouslyAdded := alreadyAddedBlobs[hash]
		skip := alreadyMirrored || previouslyAdded
		if !skip {
			// Add to tar
			d, err := digest.Parse(hash)
			if err != nil {
				return nil, fmt.Errorf("%w", err)
			}
			blobPath := filepath.Join(o.cacheDir, cacheBlobsDir, d.Algorithm().String(), d.Encoded()[:2], d.Encoded())
			err = o.adder.addAllFolder(blobPath, o.cacheDir)
			if err != nil {
				return nil, fmt.Errorf("%w", err)
			}
			blobsInDiff[hash] = ""
		}
	}
	return blobsInDiff, nil
}

// nolint: unused
func removePastArchives(destination string) error {
	_, err := os.Stat(destination)
	if err == nil {
		files, err := filepath.Glob(filepath.Join(destination, "mirror_*.tar"))
		if err != nil {
			return fmt.Errorf("%w", err)
		}
		for _, file := range files {
			err := os.Remove(file)
			if err != nil {
				return fmt.Errorf("%w", err)
			}
		}
	}
	return nil
}
