package kubeburner

import (
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

//go:embed templates/*.yml
var templateFS embed.FS

const (
	// DefaultWorkDirName is relative to the project root.
	DefaultWorkDirName = "workloads/kube-burner-node-density-cni-dpusim"

	embeddedTemplatePrefix  = "templates/"
	nodeAffinityPlaceholder = "__NODE_AFFINITY__"
)

var workloadTemplateFiles = []string{
	"node-density-cni.yml",
	"curl-deployment.yml",
	"webserver-deployment.yml",
	"webserver-service.yml",
}

var affinityTemplateFiles = []string{
	"curl-deployment.yml",
	"webserver-deployment.yml",
}

// PrepareWorkDir writes embedded workload templates into workDir and substitutes
// node affinity derived from selector into the deployment templates.
func PrepareWorkDir(workDir, selector string) error {
	if workDir == "" {
		return fmt.Errorf("work dir is empty")
	}
	affinity, err := SelectorToNodeAffinityYAML(selector)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return fmt.Errorf("create work dir: %w", err)
	}

	for _, name := range workloadTemplateFiles {
		data, err := templateFS.ReadFile(embeddedTemplatePrefix + name)
		if err != nil {
			return fmt.Errorf("read embedded template %s: %w", name, err)
		}
		dst := filepath.Join(workDir, name)
		if err := os.WriteFile(dst, data, 0o644); err != nil {
			return fmt.Errorf("write template %s: %w", dst, err)
		}
	}

	for _, name := range affinityTemplateFiles {
		path := filepath.Join(workDir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read work template %s: %w", path, err)
		}
		text := string(data)
		if !strings.Contains(text, nodeAffinityPlaceholder) {
			return fmt.Errorf("placeholder %s missing in %s", nodeAffinityPlaceholder, path)
		}
		text = strings.ReplaceAll(text, nodeAffinityPlaceholder, affinity)
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			return fmt.Errorf("write affinity into %s: %w", path, err)
		}
	}
	return nil
}
