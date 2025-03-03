package cli

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
)

type MirrorValidateInterface interface {
	CheckArgs(args []string) error
}

type MirrorValidate struct {
	Log     clog.PluggableLoggerInterface
	Options *common.MirrorOptions
}

func (o MirrorValidate) CheckArgs(args []string) error {

	keyWords := []string{
		"cluster-resources",
		"dry-run",
		"graph-preparation",
		"helm",
		"hold-operator",
		"hold-release",
		"delete",
		"logs",
		"operator-catalogs",
		"release-images",
		"signatures",
	}

	if len(o.Options.ConfigPath) == 0 {
		return fmt.Errorf("use the --config flag it is mandatory")
	}

	err := setModeAndDestination(args, o.Options)
	if err != nil {
		return err
	}

	// OCPBUGS-42862
	if strings.Contains(o.Options.OriginalDestination, fileProtocol) && o.Options.From == "" {
		if keyWord := checkKeyWord(keyWords, o.Options.OriginalDestination); len(keyWord) > 0 {
			return fmt.Errorf("the destination contains an internal oc-mirror keyword '%s'", keyWord)
		}
	}
	if len(o.Options.From) > 0 && o.Options.SinceString != "" {
		o.Log.Warn("since flag is only taken into account during mirrorToDisk workflow")
	}
	// OCPBUGS-42862
	// this should be covered in the m2d scenario, but just incase ...
	if len(o.Options.From) > 0 {
		if keyWord := checkKeyWord(keyWords, o.Options.From); len(keyWord) > 0 {
			return fmt.Errorf("the path set in --from flag contains an internal oc-mirror keyword '%s'", keyWord)
		}
	}
	if o.Options.SinceString != "" {
		if _, err := time.Parse(time.DateOnly, o.Options.SinceString); err != nil {
			return fmt.Errorf("--since flag needs to be in format yyyy-MM-dd")
		}
	}
	o.Options.MultiArch = "system"
	o.Options.RemoveSignatures = true
	o.Options.Function = mirrorFunction
	return nil
}

func checkKeyWord(key_words []string, check string) string {
	for _, i := range key_words {
		if strings.Contains(check, i) {
			return i
		}
	}
	return ""
}

func setModeAndDestination(args []string, opts *common.MirrorOptions) error {
	if opts.From != "" && opts.Workspace == "" {
		if !strings.Contains(opts.From, fileProtocol) {
			return fmt.Errorf("when using --from it must have a file:// prefix (disk-to-mirror)")
		}
		dest, ok := argsContain(args, dockerProtocol)
		if ok {
			opts.Mode = diskToMirror
			opts.WorkingDir = path.Join(strings.TrimPrefix(opts.From, fileProtocol), "working-dir")
			opts.Destination = dest
			opts.OriginalDestination = dest
			opts.DestinationRegistry = strings.TrimPrefix(dest, dockerProtocol)
		} else {
			return fmt.Errorf("when using --from ensure the destination has a docker:// prefix (disk-to-mirror)")
		}
	}
	if opts.From == "" && opts.Workspace == "" {
		dest, ok := argsContain(args, fileProtocol)
		if ok {
			opts.Mode = mirrorToDisk
			opts.WorkingDir = path.Join(strings.TrimPrefix(dest, fileProtocol), "working-dir")
			opts.Destination = strings.TrimPrefix(dest, fileProtocol)
			opts.OriginalDestination = dest
		} else {
			return fmt.Errorf("ensure destination has a file:// prefix (mirror-to-disk)")
		}
	}
	if opts.From == "" && opts.Workspace != "" {
		if strings.Contains(opts.Workspace, fileProtocol) {
			dest, ok := argsContain(args, dockerProtocol)
			if ok {
				opts.Mode = mirrorToMirror
				opts.WorkingDir = path.Join(strings.TrimPrefix(opts.Workspace, fileProtocol), "working-dir")
				opts.Destination = dest
				opts.OriginalDestination = dest
				opts.DestinationRegistry = strings.TrimPrefix(dest, dockerProtocol)
			} else {
				return fmt.Errorf("ensure the destination has a docker:// prefix (mirror-to-mirror)")
			}
		} else {
			return fmt.Errorf("when using the --workspace flag ensure it has a file:// prefix (mirror-to-mirror)")
		}
	}
	if opts.DryRun {
		opts.Mode = dryrun
	}
	return nil
}

// utility helper function
func argsContain(args []string, value string) (string, bool) {
	for _, arg := range args {
		if strings.Contains(arg, value) {
			return arg, true
		}
	}
	return "", false
}
