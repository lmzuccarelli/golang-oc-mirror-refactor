package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	//metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
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
		return result, err
	}

	if strings.Contains(string(data), "mirror:") && kind == "DeleteImageSetConfiguration" {
		return result, fmt.Errorf("mirror: is not allowed in DeleteImageSetConfigurationKind")
	}

	if strings.Contains(string(data), "delete:") && kind == "ImageSetConfiguration" {
		return result, fmt.Errorf("delete: is not allowed in ImageSetConfigurationKind")
	}

	/*
		typeMeta, err := getTypeMeta(data)
		if err != nil {
			return result, err
		}

		switch typeMeta.GroupVersionKind() {

			case v2alpha1.GroupVersion.WithKind(v2alpha1.ImageSetConfigurationKind):
				if strings.Contains(string(data), "delete:") {
					return result, fmt.Errorf("delete: is not allowed in ImageSetConfiguration")
				}
				cfg, err := LoadConfig[v2alpha1.ImageSetConfiguration](data, v2alpha1.ImageSetConfigurationKind)
				gvk := v2alpha1.GroupVersion.WithKind(v2alpha1.ImageSetConfigurationKind)
				cfg.SetGroupVersionKind(gvk)
				if err != nil {
					return result, err
				}
				Complete(&cfg)
				err = Validate(&cfg)
				if err != nil {
					return result, err
				}
				return cfg, nil
			case v2alpha1.GroupVersion.WithKind(v2alpha1.DeleteImageSetConfigurationKind):
				if strings.Contains(string(data), "mirror:") {
					return result, fmt.Errorf("mirror: is not allowed in DeleteImageSetConfiguration")
				}
				cfg, err := LoadConfig[v2alpha1.DeleteImageSetConfiguration](data, v2alpha1.DeleteImageSetConfigurationKind)
				gvk := v2alpha1.GroupVersion.WithKind(v2alpha1.DeleteImageSetConfigurationKind)
				cfg.SetGroupVersionKind(gvk)
				if err != nil {
					return result, err
				}
				CompleteDelete(&cfg)
				err = ValidateDelete(&cfg)
				if err != nil {
					return result, err
				}
				return cfg, nil
			default:
				return result, fmt.Errorf("config GVK not recognized: %s", typeMeta.GroupVersionKind())
		}
	*/
	cfg, err := LoadConfig[v2alpha1.ImageSetConfiguration](data, v2alpha1.ImageSetConfigurationKind)
	return cfg, nil
}

// LoadConfig loads data into a v2alpha1.ImageSetConfiguration or
// v2alpha1.DeleteImageSetConfiguration instance
func LoadConfig[T any](data []byte, kind string) (c T, err error) {

	if data, err = yaml.YAMLToJSON(data); err != nil {
		return c, fmt.Errorf("yaml to json %s: %v", kind, err)
	}

	var res T
	dec := json.NewDecoder(bytes.NewBuffer(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&res); err != nil {
		return c, fmt.Errorf("decode %s: %v", kind, err)
	}
	return res, nil
}

// LoadConfigDelete loads data into a v2alpha1.ImageSetConfiguration instance
func LoadConfigDelete(data []byte) (c v2alpha1.DeleteImageSetConfiguration, err error) {

	//gvk := v2alpha1.GroupVersion.WithKind(v2alpha1.DeleteImageSetConfigurationKind)

	if data, err = yaml.YAMLToJSON(data); err != nil {
		return c, fmt.Errorf("yaml to json %s: %v", "", err)
	}

	dec := json.NewDecoder(bytes.NewBuffer(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return c, fmt.Errorf("decode %s: %v", "", err)
	}

	//c.SetGroupVersionKind(gvk)

	return c, nil
}

func getTypeMeta(data []byte) (typeMeta v2alpha1.TypeMeta, err error) {
	if err := yaml.Unmarshal(data, &typeMeta); err != nil {
		return typeMeta, fmt.Errorf("get type meta: %v", err)
	}
	return typeMeta, nil
}
