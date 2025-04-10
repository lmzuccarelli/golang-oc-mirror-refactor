package cli

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
)

type DeleteValidate struct {
	Log     clog.PluggableLoggerInterface
	Options *common.MirrorOptions
}

func (o DeleteValidate) CheckArgs(args []string) error {

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

	// OCPBUGS-42862
	if strings.Contains(o.Options.OriginalDestination, fileProtocol) {
		if keyWord := checkKeyWord(keyWords, o.Options.OriginalDestination); len(keyWord) > 0 {
			return fmt.Errorf("the destination contains an internal oc-mirror keyword '%s'", keyWord)
		}
	}

	err := o.evaluateDeleteGenerate()
	if err != nil {
		return err
	}

	if o.Options.DeleteV1 && !o.Options.DeleteGenerate {
		return fmt.Errorf("the --delete-v1-images flag can only be used alongside the --generate flag")
	}

	dest, ok := argsContain(args, dockerProtocol)
	if ok {
		o.Options.WorkingDir = strings.TrimPrefix(o.Options.Workspace, fileProtocol) + "/working-dir"
		o.Options.Destination = dest
		o.Options.OriginalDestination = dest
		o.Options.DestinationRegistry = strings.TrimPrefix(dest, dockerProtocol)
	} else {
		return fmt.Errorf("ensure the destination has a docker:// prefix (mirror-to-mirror)")
	}

	deleteFile := o.Options.DeleteYaml

	_, err = os.Stat(deleteFile)
	if len(o.Options.DeleteYaml) > 0 && !o.Options.DeleteGenerate && os.IsNotExist(err) {
		return fmt.Errorf("file not found %s", deleteFile)
	}
	o.Options.Mode = diskToMirror
	o.Options.Function = deleteFunction
	o.Options.LocalStorageFQDN = "localhost:" + strconv.Itoa(int(o.Options.Port))
	o.Options.RemoveSignatures = true
	o.Options.SourceTlsVerify = false

	return nil
}

func (o DeleteValidate) evaluateDeleteGenerate() error {
	if o.Options.DeleteGenerate {
		if len(o.Options.Workspace) == 0 {
			return fmt.Errorf("use the --workspace flag, it is mandatory when using the delete command with the --generate flag")
		}
		if !strings.Contains(o.Options.Workspace, fileProtocol) {
			return fmt.Errorf("when using the --workspace flag, ensure the file:// prefix is used")
		}
		if len(o.Options.ConfigPath) == 0 {
			return fmt.Errorf("the --config flag is mandatory when used with the --generate flag")
		}
	} else if len(o.Options.DeleteYaml) == 0 {
		return fmt.Errorf("the --delete-yaml-file flag is mandatory when not using the --generate flag")
	}
	return nil
}
