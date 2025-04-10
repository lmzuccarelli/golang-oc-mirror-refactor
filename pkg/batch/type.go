package batch

import (
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
)

type ProgressStruct struct {
	Log clog.PluggableLoggerInterface
}

type StringMap map[string]string

type mirrorSchemaError struct {
	image     v2alpha1.CopyImageSchema
	err       error
	operators map[string]struct{}
	bundles   StringMap
}

func (e mirrorSchemaError) Error() string {
	return e.err.Error()
}
