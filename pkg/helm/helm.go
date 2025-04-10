package helm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	helmchart "helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	helmcli "helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/downloader"
	"helm.sh/helm/v3/pkg/engine"
	"helm.sh/helm/v3/pkg/getter"
	"helm.sh/helm/v3/pkg/releaseutil"
	helmrepo "helm.sh/helm/v3/pkg/repo"
	"k8s.io/client-go/util/jsonpath"
	"sigs.k8s.io/yaml"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/image"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
)

const (
	helmDir         string = "helm"
	helmChartDir    string = "charts"
	helmIndexesDir  string = "indexes"
	helmIndexFile   string = "index.yaml"
	dockerProtocol  string = "docker://"
	collectorPrefix string = "[HelmImageCollector] "
	errMsg          string = collectorPrefix + "%s"
)

type CollectHelm struct {
	Log     clog.PluggableLoggerInterface
	Config  v2alpha1.ImageSetConfiguration
	Options *common.MirrorOptions
}

func New(log clog.PluggableLoggerInterface, cfg v2alpha1.ImageSetConfiguration, opts *common.MirrorOptions) CollectHelm {
	return CollectHelm{
		Log:     log,
		Options: opts,
		Config:  cfg,
	}
}

func (o CollectHelm) Collect() (v2alpha1.CollectorSchema, error) {
	var (
		allImages     []v2alpha1.CopyImageSchema
		allHelmImages []v2alpha1.RelatedImage
		errs          []error
	)

	cs := v2alpha1.CollectorSchema{}

	switch {
	case o.Options.IsMirrorToDisk() || o.Options.IsMirrorToMirror():
		var err error
		imgs, errors := getHelmImagesFromLocalChart(o.Config)
		if len(errors) > 0 {
			errs = append(errs, errors...)
		}
		if len(imgs) > 0 {
			allHelmImages = append(allHelmImages, imgs...)
		}

		cleanup, file, _ := createTempFile(o.Log, filepath.Join(o.Options.WorkingDir, helmDir))
		defer cleanup()

		for _, repo := range o.Config.Mirror.Helm.Repositories {
			charts := repo.Charts

			if err := repoAdd(o.Log, repo, file); err != nil {
				errs = append(errs, err)
				continue
			}

			if charts == nil {
				var indexFile helmrepo.IndexFile
				if indexFile, err = createIndexFile(repo.URL, *o.Options); err != nil {
					errs = append(errs, err)
					continue
				}

				if charts, err = getChartsFromIndex("", indexFile, *o.Options); err != nil && charts == nil {
					errs = append(errs, err)
					continue
				}
			}

			settings := helmcli.New()
			settings.RepositoryConfig = file

			cd := downloader.ChartDownloader{
				Out:     o.Options.Stdout,
				Verify:  downloader.VerifyNever,
				Getters: getter.All(settings),
				Options: []getter.Option{
					getter.WithInsecureSkipVerifyTLS(o.Options.SourceTlsVerify),
				},
				RepositoryConfig: file,
				RepositoryCache:  settings.RepositoryCache,
			}

			for _, chart := range charts {
				o.Log.Debug("Pulling chart %s", chart.Name)
				ref := fmt.Sprintf("%s/%s", repo.Name, chart.Name)
				dest := filepath.Join(o.Options.WorkingDir, helmDir, helmChartDir)
				path, _, err := cd.DownloadTo(ref, chart.Version, dest)
				if err != nil {
					errs = append(errs, err)
					o.Log.Error("error pulling chart %s:%s", ref, err.Error())
					continue
				}

				imgs, err := getImages(path, chart.ImagePaths...)
				if err != nil {
					errs = append(errs, err)
				}
				allHelmImages = append(allHelmImages, imgs...)
			}
		}

		allImages, err = prepareM2DCopyBatch(allHelmImages, *o.Options)
		if err != nil {
			o.Log.Error(errMsg, err.Error())
			errs = append(errs, err)
		}

	case o.Options.IsDiskToMirror():
		imgs, errors := getHelmImagesFromLocalChart(o.Config)
		if len(errors) > 0 {
			errs = append(errs, errors...)
		}
		if len(imgs) > 0 {
			allHelmImages = append(allHelmImages, imgs...)
		}

		for _, repo := range o.Config.Mirror.Helm.Repositories {
			charts := repo.Charts

			if charts == nil {
				var err error
				if charts, err = getChartsFromIndex(repo.URL, helmrepo.IndexFile{}, *o.Options); err != nil {
					errs = append(errs, err)
					if charts == nil {
						continue
					}
				}
			}

			for _, chart := range charts {
				src := filepath.Join(o.Options.WorkingDir, helmDir, helmChartDir)
				path := filepath.Join(src, fmt.Sprintf("%s-%s.tgz", chart.Name, chart.Version))

				imgs, err := getImages(path, chart.ImagePaths...)
				if err != nil {
					errs = append(errs, err)
				}
				allHelmImages = append(allHelmImages, imgs...)
			}
		}

		var err error
		allImages, err = prepareD2MCopyBatch(allHelmImages, o.Options)
		if err != nil {
			o.Log.Error(errMsg, err.Error())
			errs = append(errs, err)
		}
	}
	cs.AllImages = allImages
	return cs, errors.Join(errs...)
}

func createTempFile(log clog.PluggableLoggerInterface, dir string) (func(), string, error) {
	file, err := os.CreateTemp(dir, "repo.*")
	return func() {
		if err := os.Remove(file.Name()); err != nil {
			log.Error("%s", err.Error())
		}
	}, file.Name(), fmt.Errorf("%w", err)
}

func getHelmImagesFromLocalChart(cfg v2alpha1.ImageSetConfiguration) ([]v2alpha1.RelatedImage, []error) {
	var allHelmImages []v2alpha1.RelatedImage
	var errs []error

	for _, chart := range cfg.Mirror.Helm.Local {
		imgs, err := getImages(chart.Path, chart.ImagePaths...)
		if err != nil {
			errs = append(errs, err)
		}

		if len(imgs) > 0 {
			allHelmImages = append(allHelmImages, imgs...)
		}
	}
	return allHelmImages, errs
}

func repoAdd(log clog.PluggableLoggerInterface, chartRepo v2alpha1.Repository, dir string) error {

	entry := helmrepo.Entry{
		Name: chartRepo.Name,
		URL:  chartRepo.URL,
	}

	b, err := os.ReadFile(dir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("%w", err)

	}

	var helmFile helmrepo.File
	if err := yaml.Unmarshal(b, &helmFile); err != nil {
		return fmt.Errorf("%w", err)

	}

	// Check for existing repo name
	if helmFile.Has(chartRepo.Name) {
		log.Info("repository name (%s) already exists", chartRepo.Name)
		return nil
	}

	indexDownloader, err := helmrepo.NewChartRepository(&entry, getter.All(&helmcli.EnvSettings{}))
	if err != nil {
		return fmt.Errorf("%w", err)

	}

	if _, err := indexDownloader.DownloadIndexFile(); err != nil {
		return fmt.Errorf("invalid chart repository %q: %w", chartRepo.URL, err)
	}

	// Update temp file with chart entry
	helmFile.Update(&entry)

	if err := helmFile.WriteFile(dir, 0644); err != nil {
		return fmt.Errorf("error writing helm repo file: %w", err)
	}
	return nil
}

func createIndexFile(indexURL string, opts common.MirrorOptions) (helmrepo.IndexFile, error) {
	var indexFile helmrepo.IndexFile
	if !strings.HasSuffix(indexURL, "/index.yaml") {
		indexURL += "index.yaml"
	}
	// #nosec G107
	// nolint: noctx
	resp, err := http.Get(indexURL)
	if err != nil {
		return indexFile, fmt.Errorf("%w", err)

	}
	if resp.StatusCode != 200 {
		return indexFile, fmt.Errorf("response for %v returned %v with status code %v", indexURL, resp, resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return indexFile, fmt.Errorf("%w", err)

	}
	err = yaml.Unmarshal(body, &indexFile)
	if err != nil {
		return indexFile, fmt.Errorf("%w", err)

	}

	namespace := getNamespaceFromURL(indexURL)

	indexDir := filepath.Join(opts.WorkingDir, helmDir, helmIndexesDir, namespace)

	err = os.MkdirAll(indexDir, 0755)

	if err != nil {
		return indexFile, fmt.Errorf("%w", err)

	}

	indexFilePath := filepath.Join(indexDir, "index.yaml")

	if err := indexFile.WriteFile(indexFilePath, 0644); err != nil {
		return indexFile, fmt.Errorf("error writing helm index file: %s", err.Error())
	}

	return indexFile, nil
}

func getNamespaceFromURL(url string) string {
	pathSplit := strings.Split(url, "/")
	return strings.Join(pathSplit[2:len(pathSplit)-1], "/")
}

func getChartsFromIndex(indexURL string, indexFile helmrepo.IndexFile, opts common.MirrorOptions) ([]v2alpha1.Chart, error) {
	var charts []v2alpha1.Chart

	if opts.IsDiskToMirror() {
		namespace := getNamespaceFromURL(indexURL)

		indexFilePath := filepath.Join(opts.WorkingDir, helmDir, helmIndexesDir, namespace, helmIndexFile)

		data, err := os.ReadFile(indexFilePath)
		if err != nil {
			return nil, fmt.Errorf("%w", err)

		}

		err = yaml.Unmarshal(data, &indexFile)
		if err != nil {
			return nil, fmt.Errorf("%w", err)

		}
	}

	for key, chartVersions := range indexFile.Entries {
		for _, chartVersion := range chartVersions {
			if chartVersion.Type != "library" {
				charts = append(charts, v2alpha1.Chart{Name: key, Version: chartVersion.Version})
			}
		}
	}
	return charts, nil
}

func getImages(path string, imagePaths ...string) (images []v2alpha1.RelatedImage, err error) {

	p := getImagesPath(imagePaths...)

	var chart *helmchart.Chart
	if chart, err = loader.Load(path); err != nil {
		return nil, fmt.Errorf("%w", err)

	}

	var templates string
	if templates, err = getHelmTemplates(chart); err != nil {
		return nil, fmt.Errorf("%w", err)

	}

	// Process each YAML document separately
	for _, templateData := range bytes.Split([]byte(templates), []byte("\n---\n")) {
		imgs, err := findImages(templateData, p...)

		if err != nil {
			return nil, fmt.Errorf("%w", err)

		}

		images = append(images, imgs...)
	}

	return images, nil
}

// getImagesPath returns known jsonpaths and user defined jsonpaths where images are found
// it follows the pattern of jsonpath library which is different from text/template
func getImagesPath(paths ...string) []string {
	pathlist := []string{
		"{.spec.template.spec.initContainers[*].image}",
		"{.spec.template.spec.containers[*].image}",
		"{.spec.initContainers[*].image}",
		"{.spec.containers[*].image}",
	}
	return append(pathlist, paths...)
}

// getHelmTemplates returns all chart templates
func getHelmTemplates(ch *helmchart.Chart) (string, error) {
	out := new(bytes.Buffer)
	valueOpts := make(map[string]interface{})
	caps := chartutil.DefaultCapabilities

	valuesToRender, err := chartutil.ToRenderValues(ch, valueOpts, chartutil.ReleaseOptions{}, caps)
	if err != nil {
		return "", fmt.Errorf("error rendering values: %w", err)
	}

	files, err := engine.Render(ch, valuesToRender)
	if err != nil {
		return "", fmt.Errorf("error rendering chart %s: %w", ch.Name(), err)
	}

	// Skip the NOTES.txt files
	for k := range files {
		if strings.HasSuffix(k, ".txt") {
			delete(files, k)
		}
	}

	for _, crd := range ch.CRDObjects() {
		fmt.Fprintf(out, "---\n# Source: %s\n%s\n", crd.Name, string(crd.File.Data))
	}

	_, manifests, err := releaseutil.SortManifests(files, caps.APIVersions, releaseutil.InstallOrder)
	if err != nil {
		// We return the files as a big blob of data to help the user debug parser
		// errors.
		for name, content := range files {
			if strings.TrimSpace(content) == "" {
				continue
			}
			fmt.Fprintf(out, "---\n# Source: %s\n%s\n", name, content)
		}
		return out.String(), fmt.Errorf("%w", err)

	}
	for _, m := range manifests {
		fmt.Fprintf(out, "---\n# Source: %s\n%s\n", m.Name, m.Content)
	}
	return out.String(), nil
}

// findImages will return images from parsed object
func findImages(templateData []byte, paths ...string) (images []v2alpha1.RelatedImage, err error) {

	var data interface{}
	if err := yaml.Unmarshal(templateData, &data); err != nil {
		return nil, fmt.Errorf("%w", err)

	}

	j := jsonpath.New("")
	j.AllowMissingKeys(true)

	for _, path := range paths {
		results, err := parseJSONPath(data, j, path)
		if err != nil {
			return nil, fmt.Errorf("%w", err)

		}

		for _, result := range results {
			img := v2alpha1.RelatedImage{
				Image: result,
				Type:  v2alpha1.TypeHelmImage,
			}

			images = append(images, img)
		}
	}

	return images, nil
}

// parseJSONPath will parse data and filter for a provided jsonpath template
func parseJSONPath(input interface{}, parser *jsonpath.JSONPath, template string) ([]string, error) {
	buf := new(bytes.Buffer)
	if err := parser.Parse(template); err != nil {
		return nil, fmt.Errorf("%w", err)
	}
	if err := parser.Execute(buf, input); err != nil {
		return nil, fmt.Errorf("%w", err)
	}

	f := func(s rune) bool { return s == ' ' }
	r := strings.FieldsFunc(buf.String(), f)
	return r, nil
}

func prepareM2DCopyBatch(images []v2alpha1.RelatedImage, opts common.MirrorOptions) ([]v2alpha1.CopyImageSchema, error) {
	result := []v2alpha1.CopyImageSchema{}
	for _, img := range images {
		var src string
		var dest string

		imgSpec, err := image.ParseRef(img.Image)
		if err != nil {
			return nil, fmt.Errorf("%w", err)
		}
		src = imgSpec.ReferenceWithTransport

		if imgSpec.IsImageByDigestOnly() {
			tag := fmt.Sprintf("%s-%s", imgSpec.Algorithm, imgSpec.Digest)
			if len(tag) > 128 {
				tag = tag[:127]
			}
			dest = dockerProtocol + strings.Join([]string{destinationRegistry(opts), imgSpec.PathComponent + ":" + tag}, "/")
		} else {
			dest = dockerProtocol + strings.Join([]string{destinationRegistry(opts), imgSpec.PathComponent + ":" + imgSpec.Tag}, "/")
		}

		result = append(result, v2alpha1.CopyImageSchema{Origin: img.Image, Source: src, Destination: dest, Type: img.Type})
	}
	return result, nil
}

func prepareD2MCopyBatch(images []v2alpha1.RelatedImage, opts *common.MirrorOptions) ([]v2alpha1.CopyImageSchema, error) {
	result := []v2alpha1.CopyImageSchema{}
	for _, img := range images {
		var src string
		var dest string

		imgSpec, err := image.ParseRef(img.Image)
		if err != nil {
			return nil, fmt.Errorf("%w", err)
		}
		if imgSpec.IsImageByDigestOnly() {
			tag := fmt.Sprintf("%s-%s", imgSpec.Algorithm, imgSpec.Digest)
			if len(tag) > 128 {
				tag = tag[:127]
			}
			src = dockerProtocol + strings.Join([]string{opts.LocalStorageFQDN, imgSpec.PathComponent + ":" + tag}, "/")
			if opts.WithV1Tags {
				dest = strings.Join([]string{opts.Destination, imgSpec.PathComponent + ":latest"}, "/")
			} else {
				dest = strings.Join([]string{opts.Destination, imgSpec.PathComponent + ":" + tag}, "/")
			}
		} else {
			src = dockerProtocol + strings.Join([]string{opts.LocalStorageFQDN, imgSpec.PathComponent}, "/") + ":" + imgSpec.Tag
			dest = strings.Join([]string{opts.Destination, imgSpec.PathComponent}, "/") + ":" + imgSpec.Tag
		}
		if src == "" || dest == "" {
			return result, fmt.Errorf("unable to determine src %s or dst %s for %s", src, dest, img.Name)
		}

		result = append(result, v2alpha1.CopyImageSchema{Origin: img.Image, Source: src, Destination: dest, Type: img.Type})

	}
	return result, nil
}

func destinationRegistry(opts common.MirrorOptions) string {
	var dest string
	if opts.IsDiskToMirror() || opts.IsMirrorToMirror() {
		dest = strings.TrimPrefix(opts.Destination, dockerProtocol)
	} else {
		dest = opts.LocalStorageFQDN
	}
	return dest
}
