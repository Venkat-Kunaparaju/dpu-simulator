package kubeburner

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/ovn-kubernetes/dpu-simulator/pkg/log"
	"github.com/ovn-kubernetes/dpu-simulator/pkg/platform"
)

const (
	// DefaultInstallScriptURL is the upstream one-liner from the kube-burner README.
	DefaultInstallScriptURL = "https://raw.githubusercontent.com/kube-burner/kube-burner/refs/heads/main/hack/install.sh"
)

// EnsureBinary returns an absolute path to an executable kube-burner binary.
// When binPath is set it must already exist. Otherwise PATH / ~/.local/bin is
// checked, and if missing the official install script is run (unless skipInstall).
func EnsureBinary(cmdExec platform.CommandExecutor, binPath string, skipInstall bool) (string, error) {
	if binPath != "" {
		abs, err := filepath.Abs(binPath)
		if err != nil {
			return "", fmt.Errorf("kube-burner binary path: %w", err)
		}
		if err := platform.RequireExecutable(abs); err != nil {
			return "", err
		}
		return abs, nil
	}

	if p, err := exec.LookPath("kube-burner"); err == nil {
		log.Info("Using kube-burner from PATH: %s", p)
		return p, nil
	}

	defaultBin, err := defaultInstallPath()
	if err != nil {
		return "", err
	}
	if err := platform.RequireExecutable(defaultBin); err == nil {
		log.Info("Using kube-burner at %s", defaultBin)
		return defaultBin, nil
	}

	if skipInstall {
		return "", fmt.Errorf("kube-burner not found in PATH or %s; pass --kube-burner-bin or drop --skip-install", defaultBin)
	}

	installDir := filepath.Dir(defaultBin) + string(os.PathSeparator)
	if err := os.MkdirAll(filepath.Dir(defaultBin), 0o755); err != nil {
		return "", fmt.Errorf("create kube-burner install dir: %w", err)
	}

	log.Info("Installing kube-burner via %s (INSTALL_DIR=%s)", DefaultInstallScriptURL, installDir)
	cmd := fmt.Sprintf("curl -Ls %s | INSTALL_DIR=%s bash",
		platform.ShQuote(DefaultInstallScriptURL),
		platform.ShQuote(installDir),
	)
	if _, stderr, err := cmdExec.ExecuteWithTimeout(cmd, 5*time.Minute); err != nil {
		return "", fmt.Errorf("install kube-burner: %w\n%s", err, stderr)
	}
	if err := platform.RequireExecutable(defaultBin); err != nil {
		return "", fmt.Errorf("install kube-burner: %w", err)
	}
	log.Info("kube-burner installed at %s", defaultBin)
	return defaultBin, nil
}

func defaultInstallPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, ".local", "bin", "kube-burner"), nil
}
