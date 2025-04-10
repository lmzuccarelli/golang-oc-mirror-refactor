package cli

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
)

type ValidateInterface interface {
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

	err = o.mirrorOptionsValidate()
	if err != nil {
		return err
	}

	o.Options.MultiArch = "system"
	o.Options.RemoveSignatures = true
	o.Options.Function = mirrorFunction
	return nil
}

func (o MirrorValidate) mirrorOptionsValidate() error {
	if len(o.Options.From) > 0 && o.Options.SinceString != "" {
		o.Log.Warn("since flag is only taken into account during mirrorToDisk workflow")
	}
	if o.Options.SinceString != "" {
		if _, err := time.Parse(time.DateOnly, o.Options.SinceString); err != nil {
			return fmt.Errorf("--since flag needs to be in format yyyy-MM-dd")
		}
	}
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
	set := false
	var err error
	set, err = checkAndSetModeD2M(args, opts, set)
	if err != nil {
		return err
	}
	set, err = checkAndSetModeM2D(args, opts, set)
	if err != nil {
		return err
	}
	_, err = checkAndSetModeM2M(args, opts, set)
	if err != nil {
		return err
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

func checkAndSetModeD2M(args []string, opts *common.MirrorOptions, set bool) (bool, error) {
	if !set && opts.From != "" && opts.Workspace == "" {
		if !strings.Contains(opts.From, fileProtocol) {
			return false, fmt.Errorf("when using --from it must have a file:// prefix (disk-to-mirror)")
		}
		dest, ok := argsContain(args, dockerProtocol)
		if ok {
			opts.Mode = diskToMirror
			opts.WorkingDir = path.Join(strings.TrimPrefix(opts.From, fileProtocol), "working-dir")
			opts.Destination = dest
			opts.OriginalDestination = dest
			opts.DestinationRegistry = strings.TrimPrefix(dest, dockerProtocol)
			return true, nil
		} else {
			return false, fmt.Errorf("when using --from ensure the destination has a docker:// prefix (disk-to-mirror)")
		}
	}
	return false, nil
}

func checkAndSetModeM2D(args []string, opts *common.MirrorOptions, set bool) (bool, error) {
	if !set && opts.From == "" && opts.Workspace == "" {
		dest, ok := argsContain(args, fileProtocol)
		if ok {
			opts.Mode = mirrorToDisk
			opts.WorkingDir = path.Join(strings.TrimPrefix(dest, fileProtocol), "working-dir")
			opts.Destination = strings.TrimPrefix(dest, fileProtocol)
			opts.OriginalDestination = dest
			return true, nil
		} else {
			return false, fmt.Errorf("ensure destination has a file:// prefix (mirror-to-disk)")
		}
	}
	return false, nil
}

func checkAndSetModeM2M(args []string, opts *common.MirrorOptions, set bool) (bool, error) {
	if !set && opts.From == "" && opts.Workspace != "" {
		if strings.Contains(opts.Workspace, fileProtocol) {
			dest, ok := argsContain(args, dockerProtocol)
			if ok {
				opts.Mode = mirrorToMirror
				opts.WorkingDir = path.Join(strings.TrimPrefix(opts.Workspace, fileProtocol), "working-dir")
				opts.Destination = dest
				opts.OriginalDestination = dest
				opts.DestinationRegistry = strings.TrimPrefix(dest, dockerProtocol)
				return true, nil
			} else {
				return false, fmt.Errorf("ensure the destination has a docker:// prefix (mirror-to-mirror)")
			}
		}
		return false, fmt.Errorf("when using the --workspace flag ensure it has a file:// prefix (mirror-to-mirror)")
	}
	return false, nil
}
