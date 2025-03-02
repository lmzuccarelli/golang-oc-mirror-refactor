package cli

// TODO:
// refactor to a global const in one pkg
const (
	collecAllPrefix               string = "[CollectAll] "
	dockerProtocol                string = "docker://"
	ociProtocol                   string = "oci://"
	dirProtocol                   string = "dir://"
	fileProtocol                  string = "file://"
	releaseImageDir               string = "release-images"
	logsDir                       string = "logs"
	workingDir                    string = "working-dir"
	ocmirrorRelativePath          string = ".oc-mirror"
	cacheRelativePath             string = ".oc-mirror/.cache"
	cacheEnvVar                   string = "OC_MIRROR_CACHE"
	additionalImages              string = "additional-images"
	releaseImageExtractDir        string = "hold-release"
	cincinnatiGraphDataDir        string = "cincinnati-graph-data"
	operatorImageExtractDir       string = "hold-operator"
	operatorCatalogsDir           string = "operator-catalogs"
	signaturesDir                 string = "signatures"
	registryLogFilename           string = "registry.log"
	startMessage                  string = "starting local storage on localhost:%v"
	dryRunOutDir                  string = "dry-run"
	mappingFile                   string = "mapping.txt"
	missingImgsFile               string = "missing.txt"
	clusterResourcesDir           string = "cluster-resources"
	helmDir                       string = "helm"
	helmChartDir                  string = "charts"
	helmIndexesDir                string = "indexes"
	maxParallelLayerDownloads     uint   = 10
	maxParallelImageDownloads     uint   = 8
	limitOverallParallelDownloads uint   = 200
	mirrorCommand                 string = "mirror"
	deleteCommand                 string = "delete"
	mirrorToDisk                  string = "mirror-to-disk"
	diskToMirror                  string = "disk-to-mirror"
	mirrorToMirror                string = "mirror-to-mirror"
	dryrun                        string = "mirror-to-disk"
	graphURL                      string = "https://api.openshift.com/api/upgrades_info/graph-data"
	deleteDir                     string = "/delete/"
)
