package cli

import (
	"fmt"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
)

type ValidateInterface interface {
	CheckArgs(args []string) error
}

type Validate struct {
	Log     clog.PluggableLoggerInterface
	Options *common.MirrorOptions
}

func (o Validate) CheckArgs(args []string) error {

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

	err := SetDestination(args, o.Options)
	if err != nil {
		return err
	}

	if strings.Contains(o.Options.OriginalDestination, dockerProtocol) && o.Options.WorkingDir != "" && o.Options.From != "" {
		return fmt.Errorf("when destination is docker://, --from (assumes disk to mirror workflow) and --workspace (assumes mirror to mirror workflow) cannot be used together")
	}
	if strings.Contains(o.Options.OriginalDestination, dockerProtocol) && o.Options.WorkingDir == "" && o.Options.From == "" {
		return fmt.Errorf("when destination is docker://, either --from (assumes disk to mirror workflow) or --workspace (assumes mirror to mirror workflow) need to be provided")
	}
	if strings.Contains(o.Options.OriginalDestination, fileProtocol) || strings.Contains(o.Options.OriginalDestination, dockerProtocol) {
	} else {
		return fmt.Errorf("destination must have either file:// (mirror to disk) or docker:// (diskToMirror) protocol prefixes")
	}

	SetMode(args, o.Options)

	// OCPBUGS-42862
	if strings.Contains(o.Options.OriginalDestination, fileProtocol) && o.Options.From == "" {
		if keyWord := checkKeyWord(keyWords, o.Options.OriginalDestination); len(keyWord) > 0 {
			return fmt.Errorf("the destination contains an internal oc-mirror keyword '%s'", keyWord)
		}
		o.Options.WorkingDir = path.Join(o.Options.Destination, workingDir)
		o.Options.Mode = "m2d"
		o.Options.MultiArch = "system"
	}
	if len(o.Options.From) > 0 && !strings.Contains(o.Options.From, fileProtocol) {
		return fmt.Errorf("when --from is used, it must have file:// prefix")
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

func SetDestination(args []string, opts *common.MirrorOptions) error {
	if slices.Contains(args, fileProtocol) && slices.Contains(args, dockerProtocol) {
		return fmt.Errorf("you can't have both file:// and docker:// as destination settings")
	}
	for _, arg := range args {
		if strings.Contains(arg, fileProtocol) {
			opts.Destination = strings.TrimPrefix(arg, fileProtocol)
			opts.OriginalDestination = arg
		}
		if strings.Contains(arg, dockerProtocol) {
			opts.Destination = strings.TrimPrefix(arg, dockerProtocol)
			opts.OriginalDestination = arg
		}
	}
	return nil
}

func SetMode(args []string, opts *common.MirrorOptions) {
	if opts.From != "" && slices.Contains(args, dockerProtocol) {
		opts.Mode = "d2m"
		opts.WorkingDir = strings.Split(opts.Destination, dockerProtocol)[1]
	}
	if opts.From == "" && slices.Contains(args, fileProtocol) {
		opts.Mode = "m2d"
		opts.WorkingDir = strings.Split(opts.Destination, fileProtocol)[1]
	}
	if opts.WorkingDir != "" && opts.From == "" && slices.Contains(args, dockerProtocol) {
		opts.Mode = "m2m"
	}
}
