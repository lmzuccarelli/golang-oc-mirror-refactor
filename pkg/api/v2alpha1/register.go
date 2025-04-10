package v2alpha1

// nolint: unused
const (
	version = "v2alpha1"
	group   = "mirror.openshift.io"
)

// GroupVersion contains the "group" and the "version", which uniquely identifies the API.
type GroupVersion struct {
	Group   string
	Version string
}

// nolint: unused
var (
	groupVersion = GroupVersion{Group: group, Version: version}
)
