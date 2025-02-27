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

func Execute() error {

	retry := &retry.Options{
		MaxRetry: 3,
		Delay:    time.Duration(5 * time.Second),
	}

	options := common.MirrorOptions{
		Mode:                "m2d",
		MultiArch:           "system",
		LocalStorageFQDN:    "localhost:55000",
		Destination:         "",
		DestinationRegistry: "localhost:5000",
		ConfigPath:          "",
		Terminal:            term.IsTerminal(int(os.Stdout.Fd())),
		RetryOpts:           retry,
	}

	mainCmd := flag.NewFlagSet("mirror", flag.ExitOnError)
	mainCmd.StringVar(&options.ConfigPath, "config", "", "Path to imageset configuration file")
	mainCmd.StringVar(&options.CacheDir, "cache-dir", "", "oc-mirror cache directory location. Default is $HOME")
	mainCmd.StringVar(&options.LogLevel, "log-level", "info", "Log level one of (info, debug, trace, error)")
	mainCmd.StringVar(&options.WorkingDir, "workspace", "", "oc-mirror workspace where resources and internal artifacts are generated")
	mainCmd.IntVar(&options.Port, "port", 55000, "HTTP port used by oc-mirror's local storage instance")
	mainCmd.BoolVar(&options.V2, "v2", false, "Redirect the flow to oc-mirror v2")
	mainCmd.IntVar(&options.ParallelLayerImages, "parallel-layers", 10, "Indicates the number of image layers mirrored in parallel")
	mainCmd.IntVar(&options.ParallelImages, "parallel-images", 6, "Indicates the number of images mirrored in parallel")

	// copy-only options
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
	mainCmd.BoolVar(&options.DestinationTlsVerify, "dest-tls-verify", false, "Use http (default) set to true to enable tls-verify")

	/*
		if len(os.Args) < 2 {
			fmt.Println("use --help to get list of flags and descriptions")
			os.Exit(1)
		}
	*/

	subCommand := mirrorCmd
	if os.Args[1] == deleteCmd {
		subCommand = deleteCmd
	}

	switch subCommand {

	case mirrorCmd:
		// parse command line args
		mainCmd.Parse(os.Args[1:])
		log := clog.New(options.LogLevel)
		ctx := context.Background()
		startTime := time.Now()
		// mirror flow - uses the NewMirrorFlowController
		controller := NewExecuteFlowController(ctx, log, &options)
		err := controller.MirrorProcess(mainCmd.Args())
		if err != nil {
			return err
		}
		endTime := time.Now()
		execTime := endTime.Sub(startTime)
		log.Info("mirror time     : %v", execTime)
	case deleteCmd:
		fmt.Println("subcommand 'delete'")
	default:
		return fmt.Errorf("it seems you stuffed up the command line args")
	}
	return nil
}
