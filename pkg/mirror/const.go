package mirror

// TODO:
// refactor to a global const in one pkg
const (
	MirrorToDisk        = "mirrorToDisk"
	DiskToMirror        = "diskToMirror"
	MirrorToMirror      = "mirrorToMirror"
	CopyMode       Mode = "copy"
	DeleteMode     Mode = "delete"
	CheckMode      Mode = "check"
)
