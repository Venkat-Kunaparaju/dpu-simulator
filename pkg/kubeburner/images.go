package kubeburner

import (
	"fmt"

	"github.com/ovn-kubernetes/dpu-simulator/pkg/config"
	"github.com/ovn-kubernetes/dpu-simulator/pkg/kind"
	"github.com/ovn-kubernetes/dpu-simulator/pkg/log"
	"github.com/ovn-kubernetes/dpu-simulator/pkg/platform"
)

// Workload images from the node-density-cni templates. Kept in sync with
// templates/webserver-deployment.yml and templates/curl-deployment.yml.
const (
	WebserverImage = "quay.io/cloud-bulldozer/sampleapp:latest"
	CurlImage      = "quay.io/cloud-bulldozer/curl:latest"
)

// WorkloadImages are pulled and kind-loaded before a Kind-mode run so
// podLatency excludes registry pull time (kube-burner preLoadImages cannot
// request dpusim.io/vf on DPU-host workers).
var WorkloadImages = []string{WebserverImage, CurlImage}

// preloadKindImages pulls workload images and loads them into the DPU host
// Kind cluster. No-op outside Kind mode.
func preloadKindImages(cmdExec platform.CommandExecutor, cfg *config.Config) error {
	if !cfg.IsKindMode() {
		log.Info("Skipping Kind workload image preload (not Kind mode)")
		return nil
	}

	cluster := cfg.GetDPUHostClusterName()
	if cluster == "" {
		return fmt.Errorf("no DPU host cluster for Kind workload image preload")
	}

	km, err := kind.NewKindManager(cfg)
	if err != nil {
		return fmt.Errorf("kind manager for workload image preload: %w", err)
	}

	log.Info("Preloading workload images into Kind cluster %s (kind load; preLoadImages left disabled)", cluster)
	for _, image := range WorkloadImages {
		if err := km.PullAndLoadImage(cmdExec, cluster, image); err != nil {
			return fmt.Errorf("preload %s into Kind cluster %s: %w", image, cluster, err)
		}
	}
	log.Info("✓ Workload images loaded into Kind cluster %s", cluster)
	return nil
}
