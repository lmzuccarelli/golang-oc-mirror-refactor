package cli

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/containers/common/pkg/retry"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
	"golang.org/x/term"
)

type FlowControllerInterface interface {
	Process([]string) error
}

// main execution enty point
func Execute() error {

	retry := &retry.Options{
		MaxRetry: 3,
		Delay:    time.Duration(10 * time.Second),
	}

	options := common.MirrorOptions{
		MultiArch:        "system",
		LocalStorageFQDN: "localhost:55000",
		Terminal:         term.IsTerminal(int(os.Stdout.Fd())),
		RetryOpts:        retry,
	}

	mainCmd := flag.NewFlagSet("mirror", flag.ExitOnError)
	mainCmd.StringVar(&options.ConfigPath, "config", "", "Path to imageset configuration file")
	mainCmd.StringVar(&options.CacheDir, "cache-dir", "", "oc-mirror cache directory location. Default is $HOME")
	mainCmd.StringVar(&options.LogLevel, "log-level", "info", "Log level one of (info, debug, trace, error)")
	mainCmd.StringVar(&options.Workspace, "workspace", "", "oc-mirror workspace where resources and internal artifacts are generated")
	mainCmd.IntVar(&options.Port, "port", 55000, "HTTP port used by oc-mirror's local storage instance")
	mainCmd.BoolVar(&options.V2, "v2", false, "Redirect the flow to oc-mirror v2")
	mainCmd.IntVar(&options.ParallelLayerImages, "parallel-layers", 10, "Indicates the number of image layers mirrored in parallel")
	mainCmd.IntVar(&options.ParallelImages, "parallel-images", 6, "Indicates the number of images mirrored in parallel")
	mainCmd.StringVar(&options.From, "from", "", "Local storage directory for disk to mirror workflow")
	mainCmd.BoolVar(&options.DryRun, "dry-run", false, "Print actions without mirroring images")
	mainCmd.BoolVar(&options.Quiet, "quiet", false, "Enable detailed logging when copying images")
	mainCmd.BoolVar(&options.Force, "force", false, "Force the copy and mirror functionality")
	mainCmd.StringVar(&options.SinceString, "since", "", "Include all new content since specified date (format yyyy-MM-dd). When not provided, new content since previous mirroring is mirrored")
	mainCmd.DurationVar(&options.CommandTimeout, "image-timeout", 10*time.Minute, "Timeout for mirroring an image")
	mainCmd.BoolVar(&options.SecurePolicy, "secure-policy", false, "If set, will enable signature verification (secure policy for signature verification)")
	mainCmd.IntVar(&options.MaxNestedPaths, "max-nested-paths", 0, "Number of nested paths, for destination registries that limit nested paths")
	mainCmd.BoolVar(&options.StrictArchiving, "strict-archive", false, "If set, generates archives that are strictly less than archiveSize (set in the imageSetConfig). Mirroring will exit in error if a file being archived exceed archiveSize(GB)")
	mainCmd.StringVar(&options.RootlessStoragePath, "rootless-storage-path", "", "Override the default container rootless storage path (usually in etc/containers/storage.conf)")
	mainCmd.BoolVar(&options.DestinationTlsVerify, "dest-tls-verify", false, "Use http (default) set to true to enable destination tls-verify")
	mainCmd.BoolVar(&options.SourceTlsVerify, "src-tls-verify", false, "Use http (default) set to true to enable source tls-verify")
	mainCmd.StringVar(&options.MultiArch, "multi-arch", "system", "Override by setting the value to 'all' (default is 'system')")

	deleteCmd := flag.NewFlagSet("delete", flag.ExitOnError)

	deleteCmd.StringVar(&options.ConfigPath, "config", "", "Path to delete imageset configuration file")
	deleteCmd.StringVar(&options.DeleteID, "delete-id", "", "Used to differentiate between versions for files created by the delete functionality")
	deleteCmd.StringVar(&options.DeleteYaml, "delete-yaml-file", "", "If set will use the generated or updated yaml file to delete contents")
	deleteCmd.StringVar(&options.Workspace, "workspace", "", "oc-mirror workspace where resources and internal artifacts are generated")
	deleteCmd.BoolVar(&options.ForceCacheDelete, "force-cache-delete", false, "Used to force delete  the local cache manifests and blobs")
	deleteCmd.BoolVar(&options.DeleteGenerate, "generate", false, "Used to generate the delete yaml for the list of manifests and blobs , used in the step to actually delete from local cahce and remote registry")
	deleteCmd.BoolVar(&options.DeleteV1, "delete-v1-images", false, "Used during the migration, along with --generate, in order to target images previously mirrored with oc-mirror v1")

	usage := `
	usage: oc-mirror -c <image set configuration path> [--from | --workspace] <destination prefix>:<destination location> --v2

	Command Line Examples 

	# Mirror To Disk
	oc-mirror -c ./isc.yaml file:///home/<user>/oc-mirror/mirror1 --v2

	# Disk To Mirror
	oc-mirror -c ./isc.yaml --from file:///home/<user>/oc-mirror/mirror1 docker://localhost:6000 --v2

	# Mirror To Mirror
	oc-mirror -c ./isc.yaml --workspace file:///home/<user>/oc-mirror/mirror1 docker://localhost:6000 --v2

	# Delete Phase 1 (--generate)
	oc-mirror delete -c ./delete-isc.yaml --generate --workspace file:///home/<user>/oc-mirror/delete1 --delete-id delete1-test docker://localhost:6000 --v2

	# Delete Phase 2
	oc-mirror delete --delete-yaml-file /home/<user>/oc-mirror/delete1/working-dir/delete/delete-images-delete1-test.yaml docker://localhost:6000 --v2
`

	if len(os.Args) == 1 {
		fmt.Println(usage)
		os.Exit(1)
	}

	subCommand := mirrorCommand
	if os.Args[1] == deleteCommand {
		subCommand = deleteCommand
	}

	switch subCommand {

	case mirrorCommand:
		if os.Args[1] == "--help" {
			mainCmd.PrintDefaults()
			os.Exit(0)
		}
		mainCmd.Parse(os.Args[1:])
		log := clog.New(options.LogLevel)
		log.Info("flags %t %s", options.DryRun, options.LogLevel)
		ctx := context.Background()
		startTime := time.Now()
		controller := NewMirrorFlowController(ctx, log, &options)
		err := controller.Process(mainCmd.Args())
		if err != nil {
			log.Error("%v", err)
			return err
		}
		endTime := time.Now()
		execTime := endTime.Sub(startTime)
		log.Info("mirror time     : %v", execTime)
	case deleteCommand:
		if len(os.Args) == 2 {
			fmt.Println(usage)
			os.Exit(1)
		} else if len(os.Args) > 2 {
			if os.Args[2] == "--help" {
				deleteCmd.PrintDefaults()
				os.Exit(0)
			}
		}
		deleteCmd.Parse(os.Args[2:])
		log := clog.New(options.LogLevel)
		ctx := context.Background()
		startTime := time.Now()
		controller := NewDeleteFlowController(ctx, log, &options)
		err := controller.Process(deleteCmd.Args())
		if err != nil {
			log.Error("%v", err)
			return err
		}
		endTime := time.Now()
		execTime := endTime.Sub(startTime)
		log.Info("mirror time     : %v", execTime)
	default:
		return fmt.Errorf("it seems you stuffed up the command line args")
	}
	return nil
}
