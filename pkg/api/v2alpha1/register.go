package v2alpha1

//import "k8s.io/apimachinery/pkg/runtime/schema"

const (
	version = "v2alpha1"
	group   = "mirror.openshift.io"
)

// GroupVersion contains the "group" and the "version", which uniquely identifies the API.
type GroupVersion struct {
	Group   string
	Version string
}

var (
	groupVersion = GroupVersion{Group: group, Version: version}
)
