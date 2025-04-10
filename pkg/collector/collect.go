package collector

import (
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/api/v2alpha1"
	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/common"
	clog "github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/log"
)

type CollectorManagerInterface interface {
	CollectAllImages() ([]v2alpha1.CollectorSchema, error)
	AddCollector(collector common.ImageCollectorInteface)
}

type CollectorManager struct {
	Log     clog.PluggableLoggerInterface
	Options *common.MirrorOptions
	Config  v2alpha1.ImageSetConfiguration
}

var collectors = []common.ImageCollectorInteface{}

func New(log clog.PluggableLoggerInterface, cfg v2alpha1.ImageSetConfiguration, opts *common.MirrorOptions) CollectorManager {
	collect := CollectorManager{Log: log, Config: cfg, Options: opts}
	return collect
}

func (o CollectorManager) CollectAllImages() ([]v2alpha1.CollectorSchema, error) {
	cs := []v2alpha1.CollectorSchema{}
	for _, col := range collectors {
		imgs, err := col.Collect()
		if err != nil {
			return cs, err
		}
		cs = append(cs, imgs)
	}
	return cs, nil
}

func (o CollectorManager) AddCollector(collector common.ImageCollectorInteface) {
	collectors = append(collectors, collector)
}
