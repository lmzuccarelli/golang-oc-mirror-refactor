package common

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/containers/common/pkg/retry"
	"github.com/containers/image/v5/signature"
	"github.com/containers/image/v5/types"
	"github.com/distribution/distribution/v3/registry"
	"github.com/google/uuid"
)

type MirrorOptions struct {
	MultiArch                    string
	Mode                         string
	LocalStorageFQDN             string
	Destination                  string
	OriginalDestination          string
	DestinationRegistry          string
	DestinationTlsVerify         bool
	GenerateV1DestTags           bool
	WorkingDir                   string
	GraphImage                   string
	Releases                     []string
	ConfigPath                   string
	CacheDir                     string
	LogLevel                     string
	Port                         int
	V2                           bool
	ParallelLayerImages          int
	ParallelImages               int
	Retry                        int
	From                         string
	DryRun                       bool
	Quiet                        bool
	Force                        bool
	SinceString                  string
	Since                        time.Time
	CommandTimeout               time.Duration
	SecurePolicy                 bool
	PolicyPath                   string
	MaxNestedPaths               int
	StrictArchiving              bool
	RootlessStoragePath          string
	LogsDir                      string
	LocalStorageDisk             string
	LocalStorageService          registry.Registry
	LocalStorageInterruptChannel chan error
	RegistriesDirPath            string // Path to a "registries.d" registry configuration directory
	OverrideArch                 string // Architecture to use for choosing images, instead of the runtime one
	OverrideOS                   string // OS to use for choosing images, instead of the runtime one
	OverrideVariant              string // Architecture variant to use for choosing images, instead of the runtime one
	RegistriesConfPath           string // Path to the "registries.conf" file
	TmpDir                       string // Path to use for big temporary files
	Terminal                     bool
	PreserveDigests              bool
	Function                     string
	AdditionalTags               []string  // For docker-archive: destinations, in addition to the name:tag specified as destination, also add these
	RemoveSignatures             bool      // Do not copy signatures from the source image
	SignByFingerprint            string    // Sign the image using a GPG key with the specified fingerprint
	SignBySigstorePrivateKey     string    // Sign the image using a sigstore private key
	SignPassphraseFile           string    // Path pointing to a passphrase file when signing (for either signature format, but only one of them)
	SignIdentity                 string    // Identity of the signed image, must be a fully specified docker reference
	DigestFile                   string    // Write digest to this file
	Format                       string    // Force conversion of the image to a specified format
	All                          bool      // Copy all of the images if the source is a list
	EncryptLayer                 []int     // The list of layers to encrypt
	EncryptionKeys               []string  // Keys needed to encrypt the image
	DecryptionKeys               []string  // Keys needed to decrypt the image
	IsDryRun                     bool      // generates a mappings.txt without performing the mirroring
	Dev                          bool      // developer mode - will be removed when completed
	UUID                         uuid.UUID // set uuid
	ImageType                    string    // release, catalog-operator, additionalImage
	Stdout                       io.Writer
	DeprecatedTLSVerify          bool
	RetryOpts                    *retry.Options
	deprecatedTLSVerify          bool   // May be shared across several imageOptions instances, or nil.
	authFilePath                 string // Path to a */containers/auth.json (prefixed version to override shared image option).
	credsOption                  string // username[:password] for accessing a registry
	userName                     string // username for accessing a registry
	password                     string // password for accessing a registry
	registryToken                string // token to be used directly as a Bearer token when accessing the registry
	dockerCertPath               string // A directory using Docker-like *.{crt,cert,key} files for connecting to a registry or a daemon
	TlsVerify                    bool   // Require HTTPS and verify certificates (for docker: and docker-daemon:)
	noCreds                      bool   // Access the registry anonymously
	sharedBlobDir                string // A directory to use for OCI blobs, shared across repositories
	dockerDaemonHost             string // docker-daemon: host to connect to
	dirForceCompression          bool   // Compress layers when saving to the dir: transport
	dirForceDecompression        bool   // Decompress layers when saving to the dir: transport
	ociAcceptUncompressedLayers  bool   // Whether to accept uncompressed layers in the oci: transport
	compressionFormat            string // Format to use for the compression
	compressionLevel             int    // Level to use for the compression
	precomputeDigests            bool   // Precompute digests to dedup layers when saving to the docker: transport
	RegistryLogFile              *os.File
}

const defaultUserAgent string = "oc-mirror"

// errorShouldDisplayUsage is a subtype of error used by command handlers to indicate that cli.ShowSubcommandHelp should be called.
type ErrorShouldDisplayUsage struct {
	Error error
}

// getPolicyContext returns a *signature.PolicyContext based on opts.
func (opts MirrorOptions) GetPolicyContext() (*signature.PolicyContext, error) {
	var policy *signature.Policy // This could be cached across calls in opts.
	var err error
	if !opts.SecurePolicy {
		policy = &signature.Policy{Default: []signature.PolicyRequirement{signature.NewPRInsecureAcceptAnything()}}
	} else if opts.PolicyPath == "" {
		policy, err = signature.DefaultPolicy(nil)
	} else {
		policy, err = signature.NewPolicyFromFile(opts.PolicyPath)
	}
	if err != nil {
		return nil, err
	}
	return signature.NewPolicyContext(policy)
}

// commandTimeoutContext returns a context.Context and a cancellation callback based on opts.
// The caller should usually "defer cancel()" immediately after calling this.
func (opts MirrorOptions) CommandTimeoutContext() (context.Context, context.CancelFunc) {
	ctx := context.Background()
	var cancel context.CancelFunc = func() {
		// empty function - its ok for now
	}
	if opts.CommandTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, opts.CommandTimeout)
	}
	return ctx, cancel
}

// newSystemContext returns a *types.SystemContext corresponding to opts.
// It is guaranteed to return a fresh instance, so it is safe to make additional updates to it.
func (opts MirrorOptions) NewSystemContext() *types.SystemContext {
	ctx := &types.SystemContext{
		RegistriesDirPath:        opts.RegistriesDirPath,
		ArchitectureChoice:       opts.OverrideArch,
		OSChoice:                 opts.OverrideOS,
		VariantChoice:            opts.OverrideVariant,
		SystemRegistriesConfPath: opts.RegistriesConfPath,
		BigFilesTemporaryDir:     opts.TmpDir,
		DockerRegistryUserAgent:  defaultUserAgent,
		DockerCertPath:           opts.dockerCertPath,
		OCISharedBlobDirPath:     opts.sharedBlobDir,
		AuthFilePath:             opts.authFilePath,
		DockerDaemonHost:         opts.dockerDaemonHost,
		DockerDaemonCertPath:     opts.dockerCertPath,
	}

	return ctx
}

func parseCreds(creds string) (string, string, error) {
	if creds == "" {
		return "", "", errors.New("credentials can't be empty")
	}
	up := strings.SplitN(creds, ":", 2)
	if len(up) == 1 {
		return up[0], "", nil
	}
	if up[0] == "" {
		return "", "", errors.New("username can't be empty")
	}
	return up[0], up[1], nil
}

func getDockerAuth(creds string) (*types.DockerAuthConfig, error) {
	username, password, err := parseCreds(creds)
	if err != nil {
		return nil, err
	}
	return &types.DockerAuthConfig{
		Username: username,
		Password: password,
	}, nil
}

func (o MirrorOptions) IsTerminal() bool {
	return o.Terminal
}

func (o MirrorOptions) IsMirrorToDisk() bool {
	return o.Mode == "m2d"
}

func (o MirrorOptions) IsMirrorToMirror() bool {
	return o.Mode == "m2m"
}

func (o MirrorOptions) IsDiskToMirror() bool {
	return o.Mode == "d2m"
}

func (o MirrorOptions) IsDelete() bool {
	return o.Function == "delete"
}

func (c MirrorOptions) IsCopy() bool {
	return c.Function == "copy"
}
