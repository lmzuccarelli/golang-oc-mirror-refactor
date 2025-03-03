package mirror

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/containers/common/pkg/retry"
	"github.com/containers/image/v5/copy"
	"github.com/containers/image/v5/pkg/cli"

	"github.com/containers/image/v5/manifest"
	"github.com/containers/image/v5/transports/alltransports"
	"github.com/containers/image/v5/types"
	"github.com/distribution/reference"
	//"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
	imgspecv1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type Mode string

type MirrorInterface interface {
	Copy(ctx context.Context, src, dest string, opts *common.MirrorOptions) (retErr error)
	Delete(ctx context.Context, dest string, opts *common.MirrorOptions) (retErr error)
}

type MirrorController struct {
	Log     clog.PluggableLoggerInterface
	Options *common.MirrorOptions
	Context context.Context
}

func New(ctx context.Context, log clog.PluggableLoggerInterface, opts *common.MirrorOptions) MirrorInterface {
	mirror := MirrorController{Context: ctx, Log: log, Options: opts}
	return mirror
}

func (o MirrorController) Copy(ctx context.Context, src, dest string, opts *common.MirrorOptions) (retErr error) {

	if err := ReexecIfNecessaryForImages([]string{src, dest}...); err != nil {
		return err
	}

	policyContext, err := opts.GetPolicyContext()
	if err != nil {
		return fmt.Errorf("error loading trust policy: %v", err)
	}
	defer func() {
		if err := policyContext.Destroy(); err != nil {
			retErr = NoteCloseFailure(retErr, "tearing down policy context", err)
		}
	}()

	srcRef, err := alltransports.ParseImageName(src)
	if err != nil {
		return fmt.Errorf("invalid source name %s: %v", src, err)
	}
	destRef, err := alltransports.ParseImageName(dest)
	if err != nil {
		return fmt.Errorf("invalid destination name %s: %v", dest, err)
	}

	sourceCtx := opts.NewSystemContext()
	//if err != nil {
	//	return err
	//}
	if strings.Contains(src, opts.LocalStorageFQDN) { // when copying from cache, use HTTP
		sourceCtx.DockerInsecureSkipTLSVerify = types.OptionalBoolTrue
	}

	destinationCtx := opts.NewSystemContext()
	//if err != nil {
	//	return err
	//}

	if strings.Contains(dest, opts.LocalStorageFQDN) {
		// when copying to cache, use HTTP
		destinationCtx.DockerInsecureSkipTLSVerify = types.OptionalBoolTrue
	}

	var manifestType string
	if len(opts.Format) > 0 {
		manifestType, err = ParseManifestFormat(opts.Format)
		if err != nil {
			return err
		}
	}

	imageListSelection := copy.CopySystemImage
	if len(opts.MultiArch) > 0 && opts.All {
		return fmt.Errorf("cannot use --all and --multi-arch flags together")
	}

	if len(opts.MultiArch) > 0 {
		imageListSelection, err = parseMultiArch(opts.MultiArch)
		if err != nil {
			return err
		}
	}

	if opts.All {
		imageListSelection = copy.CopyAllImages
	}

	if len(opts.EncryptionKeys) > 0 && len(opts.DecryptionKeys) > 0 {
		return fmt.Errorf("--encryption-key and --decryption-key cannot be specified together")
	}

	// c/image/copy.Image does allow creating both simple signing and sigstore signatures simultaneously,
	// with independent passphrases, but that would make the CLI probably too confusing.
	// For now, use the passphrase with either, but only one of them.
	if opts.SignPassphraseFile != "" && opts.SignByFingerprint != "" && opts.SignBySigstorePrivateKey != "" {
		return fmt.Errorf("only one of --sign-by and sign-by-sigstore-private-key can be used with sign-passphrase-file")
	}
	var passphrase string
	if opts.SignPassphraseFile != "" {
		p, err := cli.ReadPassphraseFile(opts.SignPassphraseFile)
		if err != nil {
			return err
		}
		passphrase = p
	}

	// opts.signByFingerprint triggers a GPG-agent passphrase prompt, possibly using a more secure channel,
	// so we usually shouldn’t prompt ourselves if no passphrase was explicitly provided.
	var signIdentity reference.Named = nil
	if opts.SignIdentity != "" {
		signIdentity, err = reference.ParseNamed(opts.SignIdentity)
		if err != nil {
			return fmt.Errorf("could not parse --sign-identity: %v", err)
		}
	}

	// hard coded ReportWriter to io.Discard
	co := &copy.Options{
		RemoveSignatures:                 opts.RemoveSignatures,
		SignBy:                           opts.SignByFingerprint,
		SignPassphrase:                   passphrase,
		SignBySigstorePrivateKeyFile:     opts.SignBySigstorePrivateKey,
		SignSigstorePrivateKeyPassphrase: []byte(passphrase),
		SignIdentity:                     signIdentity,
		ReportWriter:                     io.Discard,
		SourceCtx:                        sourceCtx,
		DestinationCtx:                   destinationCtx,
		ForceManifestMIMEType:            manifestType,
		ImageListSelection:               imageListSelection,
		PreserveDigests:                  opts.PreserveDigests,
		MaxParallelDownloads:             uint(opts.ParallelLayerImages),
	}

	if opts.LogLevel == "debug" {
		co.ReportWriter = opts.Stdout
	}

	return retry.IfNecessary(ctx, func() error {

		manifestBytes, err := copy.Image(ctx, policyContext, destRef, srcRef, co)
		if err != nil {
			return err
		}
		if opts.DigestFile != "" {
			manifestDigest, err := manifest.Digest(manifestBytes)
			if err != nil {
				return err
			}
			if err = os.WriteFile(opts.DigestFile, []byte(manifestDigest.String()), 0644); err != nil {
				return fmt.Errorf("failed to write digest to file %q: %w", opts.DigestFile, err)
			}
		}
		return nil
	}, opts.RetryOpts)
}

// check exists - checks if image exists
func Check(ctx context.Context, image string, opts *common.MirrorOptions, asCopySrc bool) (bool, error) {

	if err := ReexecIfNecessaryForImages([]string{image}...); err != nil {
		return false, err
	}

	imageRef, err := alltransports.ParseImageName(image)
	if err != nil {
		return false, fmt.Errorf("invalid source name %s: %v", image, err)
	}
	var sysCtx *types.SystemContext
	if asCopySrc {
		sysCtx = opts.NewSystemContext()
	} else {
		sysCtx = opts.NewSystemContext()
	}

	ctx, cancel := opts.CommandTimeoutContext()
	defer cancel()

	err = retry.IfNecessary(ctx, func() error {
		_, err := imageRef.NewImageSource(ctx, sysCtx)
		if err != nil {
			return err
		}
		return nil
	}, opts.RetryOpts)

	if err == nil {
		return true, nil
	} else if strings.Contains(err.Error(), "manifest unknown") {
		return false, nil
	} else {
		return false, err
	}
}

// delete - delete images
func (o MirrorController) Delete(ctx context.Context, image string, opts *common.MirrorOptions) error {

	if err := ReexecIfNecessaryForImages([]string{image}...); err != nil {
		return err
	}

	imageRef, err := alltransports.ParseImageName(image)
	if err != nil {
		return fmt.Errorf("invalid source name %s: %v", image, err)
	}

	sysCtx := opts.NewSystemContext()

	if strings.Contains(image, opts.LocalStorageFQDN) { // when copying to cache, use HTTP
		sysCtx.DockerInsecureSkipTLSVerify = types.OptionalBoolTrue
	}

	return retry.IfNecessary(ctx, func() error {

		err := imageRef.DeleteImage(ctx, sysCtx)
		if err != nil {
			return err
		}
		return nil
	}, opts.RetryOpts)
}

// parseMultiArch
func parseMultiArch(multiArch string) (copy.ImageListSelection, error) {
	switch multiArch {
	case "system":
		return copy.CopySystemImage, nil
	case "all":
		return copy.CopyAllImages, nil
	// There is no CopyNoImages value in copy.ImageListSelection, but because we
	// don't provide an option to select a set of images to copy, we can use
	// CopySpecificImages.
	case "index-only":
		return copy.CopySpecificImages, nil
	// We don't expose CopySpecificImages other than index-only above, because
	// we currently don't provide an option to choose the images to copy. That
	// could be added in the future.
	default:
		return copy.CopySystemImage, fmt.Errorf("unknown multi-arch option %q. Choose one of the supported options: 'system', 'all', or 'index-only'", multiArch)
	}
}

// noteCloseFailure returns (possibly-nil) err modified to account for (non-nil) closeErr.
// The error for closeErr is annotated with description (which is not a format string)
// Typical usage:
//
//	defer func() {
//		if err := something.Close(); err != nil {
//			returnedErr = noteCloseFailure(returnedErr, "closing something", err)
//		}
//	}
func NoteCloseFailure(err error, description string, closeErr error) error {
	// We don’t accept a Closer() and close it ourselves because signature.PolicyContext has .Destroy(), not .Close().
	// This also makes it harder for a caller to do
	//     defer noteCloseFailure(returnedErr, …)
	// which doesn’t use the right value of returnedErr, and doesn’t update it.
	if err == nil {
		return fmt.Errorf("%s: %w", description, closeErr)
	}
	// In this case we prioritize the primary error for use with %w; closeErr is usually less relevant, or might be a consequence of the primary erorr.
	return fmt.Errorf("%w (%s: %v)", err, description, closeErr)
}

// parseManifestFormat parses format parameter for copy and sync command.
// It returns string value to use as manifest MIME type
func ParseManifestFormat(manifestFormat string) (string, error) {
	switch manifestFormat {
	case "oci":
		return imgspecv1.MediaTypeImageManifest, nil
	case "v2s1":
		return manifest.DockerV2Schema1SignedMediaType, nil
	case "v2s2":
		return manifest.DockerV2Schema2MediaType, nil
	default:
		return "", fmt.Errorf("unknown format %q. Choose one of the supported formats: 'oci', 'v2s1', or 'v2s2'", manifestFormat)
	}
}
