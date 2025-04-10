package manifest

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/containers/image/v5/manifest"
	"github.com/containers/image/v5/transports/alltransports"
	"github.com/containers/image/v5/types"
	digest "github.com/opencontainers/go-digest"
	"github.com/otiai10/copy"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/mirror"
)

// GetImageIndex - used to get the oci index.json
func GetImageIndex(dir string) (*v2alpha1.OCISchema, error) {

	var oci *v2alpha1.OCISchema
	indx, err := os.ReadFile(dir + "/" + index)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}
	err = json.Unmarshal(indx, &oci)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}
	return oci, nil
}

// GetImageManifest used to ge the manifest in the oci blobs/sha254
// directory - found in index.json
func GetImageManifest(file string) (*v2alpha1.OCISchema, error) {
	var oci *v2alpha1.OCISchema
	manifest, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}
	err = json.Unmarshal(manifest, &oci)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}
	return oci, nil
}

// GetOperatorConfig used to parse the operator json
func GetOperatorConfig(file string) (*v2alpha1.OperatorConfigSchema, error) {
	var ocs *v2alpha1.OperatorConfigSchema
	manifest, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}
	err = json.Unmarshal(manifest, &ocs)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}
	return ocs, nil
}

// ExtractLayersOCI
func ExtractLayersOCI(fromPath, toPath, label string, oci *v2alpha1.OCISchema) error {
	if _, err := os.Stat(toPath + "/" + label); errors.Is(err, os.ErrNotExist) {
		for _, blob := range oci.Layers {
			validDigest, err := digest.Parse(blob.Digest)
			if err != nil {
				return fmt.Errorf("the digest format is not correct %s ", blob.Digest)
			}
			f, err := os.Open(fromPath + "/" + validDigest.Encoded())
			if err != nil {
				return fmt.Errorf("%w", err)
			}
			err = untar(f, toPath, label)
			if err != nil {
				return fmt.Errorf("%w", err)
			}
		}
	}
	return nil
}

// GetReleaseSchema
func GetReleaseSchema(filePath string) ([]v2alpha1.RelatedImage, error) {
	var release = v2alpha1.ReleaseSchema{}

	file, err := os.ReadFile(filePath)
	if err != nil {
		return []v2alpha1.RelatedImage{}, fmt.Errorf("%w", err)
	}

	err = json.Unmarshal([]byte(file), &release)
	if err != nil {
		return []v2alpha1.RelatedImage{}, fmt.Errorf("%w", err)
	}

	allImages := []v2alpha1.RelatedImage{}
	for _, item := range release.Spec.Tags {
		allImages = append(allImages, v2alpha1.RelatedImage{Image: item.From.Name, Name: item.Name, Type: v2alpha1.TypeOCPReleaseContent})
	}
	return allImages, nil
}

// UntarLayers simple function that untars the image layers
func untar(gzipStream io.Reader, path string, cfgDirName string) error {
	// Remove any separators in cfgDirName as received from the label
	cfgDirName = strings.TrimSuffix(cfgDirName, "/")
	cfgDirName = strings.TrimPrefix(cfgDirName, "/")
	uncompressedStream, err := gzip.NewReader(gzipStream)
	if err != nil {
		return fmt.Errorf("untar: gzipStream - %w", err)
	}

	tarReader := tar.NewReader(uncompressedStream)
	for {
		header, err := tarReader.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			return fmt.Errorf("untar: Next() failed: %s", err.Error())
		}

		if strings.Contains(header.Name, cfgDirName) {
			switch header.Typeflag {
			case tar.TypeDir:
				if header.Name != "./" {
					// #nosec G305
					if err := os.MkdirAll(filepath.Join(path, header.Name), 0755); err != nil {
						return fmt.Errorf("untar: Mkdir() failed: %w", err)
					}
				}
			case tar.TypeReg:
				// #nosec G305
				err := os.MkdirAll(filepath.Dir(filepath.Join(path, header.Name)), 0755)
				if err != nil {
					return fmt.Errorf("untar: Create() failed: %w", err)
				}
				// #nosec G305
				outFile, err := os.Create(filepath.Join(path, header.Name))
				if err != nil {
					return fmt.Errorf("untar: Create() failed: %w", err)
				}
				// #nosec G110
				if _, err := io.Copy(outFile, tarReader); err != nil {
					return fmt.Errorf("untar: Copy() failed: %w", err)
				}
				outFile.Close()

			default:
				// just ignore errors as we are only interested in the FB configs layer
			}
		}
	}
	return nil
}

// ConvertIndex converts the index.json to a single manifest which refers to a multi manifest index in the blobs/sha256 directory
// this is necessary because containers/image does not support multi manifest indexes on the top level folder
func ConvertIndexToSingleManifest(dir string, oci *v2alpha1.OCISchema) error {

	data, _ := os.ReadFile(path.Join(dir, "index.json"))
	hash := sha256.Sum256(data)
	digest := hex.EncodeToString(hash[:])
	size := len(data)

	err := copy.Copy(path.Join(dir, "index.json"), path.Join(dir, "blobs", "sha256", digest))
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	idx := v2alpha1.OCISchema{
		SchemaVersion: oci.SchemaVersion,
		Manifests:     []v2alpha1.OCIManifest{{MediaType: oci.MediaType, Digest: "sha256:" + digest, Size: size}},
	}

	idxData, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	// Write the JSON string to a file
	// #nosec G306
	err = os.WriteFile(path.Join(dir, "index.json"), idxData, 0644)
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	return nil
}

func GetDigest(ctx context.Context, sourceCtx *types.SystemContext, imgRef string) (string, error) {

	if err := mirror.ReexecIfNecessaryForImages([]string{imgRef}...); err != nil {
		return "", fmt.Errorf("%w", err)
	}

	srcRef, err := alltransports.ParseImageName(imgRef)
	if err != nil {
		return "", fmt.Errorf("invalid source name %s: %w", imgRef, err)
	}

	img, err := srcRef.NewImageSource(ctx, sourceCtx)
	if err != nil {
		return "", fmt.Errorf("%w", err)
	}

	manifestBytes, _, err := img.GetManifest(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("%w", err)
	}

	digest, err := manifest.Digest(manifestBytes)
	if err != nil {
		return "", fmt.Errorf("%w", err)
	}

	var digestString string
	if strings.Contains(digest.String(), ":") {
		digestString = strings.Split(digest.String(), ":")[1]
	}

	return digestString, nil
}
