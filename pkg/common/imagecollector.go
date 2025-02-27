package common

import (
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
)

type ImageCollectorInteface interface {
	Collect() ([]v2alpha1.CopyImageSchema, error)
}
