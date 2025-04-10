package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
	"sigs.k8s.io/yaml"
)

type ConfigInterface interface {
	Read()
}

type Config struct {
}

// ReadConfig opens an imageset configuration file at the given path
// and loads it into a v2alpha1.ImageSetConfiguration instance for processing and validation.
func (o Config) Read(configPath string, kind string) (interface{}, error) {

	result := interface{}(nil)
	data, err := os.ReadFile(filepath.Clean(configPath))
	if err != nil {
		return result, fmt.Errorf("%w", err)
	}

	if strings.Contains(string(data), "mirror:") && kind == "DeleteImageSetConfiguration" {
		return result, fmt.Errorf("mirror: is not allowed in DeleteImageSetConfigurationKind")
	}

	if strings.Contains(string(data), "delete:") && kind == "ImageSetConfiguration" {
		return result, fmt.Errorf("delete: is not allowed in ImageSetConfigurationKind")
	}
	switch kind {
	case v2alpha1.ImageSetConfigurationKind:
		cfg, err := LoadConfig[v2alpha1.ImageSetConfiguration](data, v2alpha1.ImageSetConfigurationKind)
		if err != nil {
			return nil, fmt.Errorf("%w", err)
		}
		return cfg, nil
	case v2alpha1.DeleteImageSetConfigurationKind:
		cfg, err := LoadConfig[v2alpha1.DeleteImageSetConfiguration](data, v2alpha1.DeleteImageSetConfigurationKind)
		if err != nil {
			return nil, fmt.Errorf("%w", err)
		}
		return cfg, nil
	}
	return nil, fmt.Errorf("could not parse imagesetconfiguration ")
}

// LoadConfig loads data into a v2alpha1.ImageSetConfiguration or
// v2alpha1.DeleteImageSetConfiguration instance
// nolint: ireturn
func LoadConfig[T any](data []byte, kind string) (c T, err error) {

	if data, err = yaml.YAMLToJSON(data); err != nil {
		return c, fmt.Errorf("yaml to json %s: %w", kind, err)
	}
	var res T
	dec := json.NewDecoder(bytes.NewBuffer(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&res); err != nil {
		return c, fmt.Errorf("decode %s: %w", kind, err)
	}
	return res, nil
}

// LoadConfigDelete loads data into a v2alpha1.ImageSetConfiguration instance
func LoadConfigDelete(data []byte) (c v2alpha1.DeleteImageSetConfiguration, err error) {

	if data, err = yaml.YAMLToJSON(data); err != nil {
		return c, fmt.Errorf("yaml to json %s: %w", "", err)
	}

	dec := json.NewDecoder(bytes.NewBuffer(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return c, fmt.Errorf("decode %s: %w", "", err)
	}

	return c, nil
}

// nolint: unused
func getTypeMeta(data []byte) (typeMeta v2alpha1.TypeMeta, err error) {
	if err := yaml.Unmarshal(data, &typeMeta); err != nil {
		return typeMeta, fmt.Errorf("get type meta: %w", err)
	}
	return typeMeta, nil
}
