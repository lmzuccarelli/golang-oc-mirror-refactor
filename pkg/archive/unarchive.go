package archive

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type MirrorUnArchiver struct {
	UnArchiver
	workingDir   string
	cacheDir     string
	archiveFiles []string
}

func NewArchiveExtractor(archivePath, workingDir, cacheDir string) (MirrorUnArchiver, error) {
	ae := MirrorUnArchiver{
		workingDir: workingDir,
		cacheDir:   cacheDir,
	}
	files, err := os.ReadDir(archivePath)
	if err != nil {
		return MirrorUnArchiver{}, fmt.Errorf("%w", err)
	}

	//nolint: gocritic
	rxp, err := regexp.Compile(archiveFilePrefix + "_[0-9]{6}\\.tar")
	if err != nil {
		return MirrorUnArchiver{}, fmt.Errorf("%w", err)
	}
	for _, chunk := range files {

		if rxp.MatchString(chunk.Name()) {
			ae.archiveFiles = append(ae.archiveFiles, filepath.Join(archivePath, chunk.Name()))
		}
	}
	return ae, nil
}

// Unarchive extracts:
// * docker/v2* to cacheDir
// * working-dir to workingDir
func (o MirrorUnArchiver) Unarchive() error {
	for _, chunkPath := range o.archiveFiles {
		chunkFile, err := os.Open(chunkPath)
		if err != nil {
			return fmt.Errorf("%w", err)
		}
		defer chunkFile.Close()
		reader := tar.NewReader(chunkFile)
		// make sure workingDir exists
		err = os.MkdirAll(o.workingDir, 0755)
		if err != nil {
			return fmt.Errorf(errMessageFolder, o.workingDir, err)
		}
		// make sure cacheDir exists
		err = os.MkdirAll(o.cacheDir, 0755)
		if err != nil {
			return fmt.Errorf(errMessageFolder, o.cacheDir, err)
		}
		for {
			header, err := reader.Next()

			// break the infinite loop when EOF
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				return fmt.Errorf("error reading archive %s: %w", chunkFile.Name(), err)
			}

			if header == nil {
				continue
			}
			// taking only files into account
			// because we are considering that all parent folders will be
			// created recursively, and that, to the best of our knowledge
			// the archive doesn't include any symbolic links

			// for the moment we ignore imageSetConfig that is
			// included in the tar
			// as well as any other files that are not
			// working-dir or cache

			if header.Typeflag == tar.TypeReg {
				descriptor := ""
				// case file belongs to working-dir
				// nolint: gocritic
				if strings.Contains(header.Name, workingDirectory) {
					workingDirParent := filepath.Dir(o.workingDir)
					// #nosec G305
					descriptor = filepath.Join(workingDirParent, header.Name)
				} else if strings.Contains(header.Name, cacheFilePrefix) {
					// case file belongs to the cache
					// #nosec G305
					descriptor = filepath.Join(o.cacheDir, header.Name)
				} else {
					continue
				}
				// make sure all the parent directories exist
				descriptorParent := filepath.Dir(descriptor)
				if err := os.MkdirAll(descriptorParent, 0755); err != nil {
					return fmt.Errorf(errMessageFolder, descriptorParent, err)
				}
				// if it's a file create it, making sure it's at least writable and executable by the user
				// since with every UnArchive, we should be able to rewrite the file
				// #nosec G115
				f, err := os.OpenFile(descriptor, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode)|0755)
				if err != nil {
					return fmt.Errorf("unable to create file %s: %w", descriptor, err)
				}
				// copy  contents
				// #nosec G110
				if _, err := io.Copy(f, reader); err != nil { // #nosec G115
					return fmt.Errorf("error copying file %s: %w", descriptor, err)
				}

				// manually close here after each file operation; defering would cause each file close
				// to wait until all operations have completed.
				f.Close()

			}
		}
	}

	return nil
}
