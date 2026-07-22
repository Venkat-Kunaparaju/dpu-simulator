package kubeburner

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ovn-kubernetes/dpu-simulator/pkg/config"

	"github.com/stretchr/testify/require"
)

func TestComputeIterations(t *testing.T) {
	t.Parallel()
	got, err := ComputeIterations(2, 45)
	require.NoError(t, err)
	require.Equal(t, 45, got)

	got, err = ComputeIterations(2, 10)
	require.NoError(t, err)
	require.Equal(t, 10, got)

	_, err = ComputeIterations(0, 45)
	require.Error(t, err)
}

func TestResolveQPSBurst(t *testing.T) {
	t.Parallel()
	qps, burst := ResolveQPSBurst(0, 0)
	require.Equal(t, DefaultQPS, qps)
	require.Equal(t, DefaultBurst, burst)

	qps, burst = ResolveQPSBurst(8, 0)
	require.Equal(t, 8, qps)
	require.Equal(t, 8, burst)

	qps, burst = ResolveQPSBurst(8, 16)
	require.Equal(t, 8, qps)
	require.Equal(t, 16, burst)
}

func TestResolveKubeconfigPath_Override(t *testing.T) {
	tmp := t.TempDir()
	kc := filepath.Join(tmp, "custom.kubeconfig")
	require.NoError(t, os.WriteFile(kc, []byte("x"), 0o600))

	cfg := &config.Config{
		Kubernetes: config.KubernetesConfig{
			Clusters: []config.ClusterConfig{{Name: "host-c"}},
		},
	}
	got, err := ResolveKubeconfigPath(cfg, kc)
	require.NoError(t, err)
	require.Equal(t, kc, got)
}

func TestResolveKubeconfigPath_DPUHostCluster(t *testing.T) {
	tmp := t.TempDir()
	cfg := &config.Config{
		Kind: &config.KindConfig{Nodes: []config.KindNodeConfig{
			{Name: "cp-dpu", K8sCluster: "dpu-c", K8sRole: "control-plane"},
			{Name: "dw", Type: config.DpuType, K8sCluster: "dpu-c", K8sRole: "worker", Host: "hw"},
			{Name: "cp-host", K8sCluster: "host-c", K8sRole: "control-plane"},
			{Name: "hw", Type: config.HostType, K8sCluster: "host-c", K8sRole: "worker"},
		}},
		Kubernetes: config.KubernetesConfig{
			KubeconfigDir: "kubeconfig",
			Clusters: []config.ClusterConfig{
				{Name: "dpu-c", CNI: config.CNIFlannel},
				{Name: "host-c", CNI: config.CNIOVNKubernetes},
			},
		},
	}
	oldWd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(tmp))
	defer func() { _ = os.Chdir(oldWd) }()
	require.NoError(t, os.MkdirAll("kubeconfig", 0o755))
	want := filepath.Join(tmp, "kubeconfig", "host-c.kubeconfig")
	require.NoError(t, os.WriteFile(want, []byte("x"), 0o600))

	got, err := ResolveKubeconfigPath(cfg, "")
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestResolveKubeconfigPath_MissingDPUHostCluster(t *testing.T) {
	cfg := &config.Config{
		Kubernetes: config.KubernetesConfig{
			Clusters: []config.ClusterConfig{{Name: "only-cluster"}},
		},
	}
	_, err := ResolveKubeconfigPath(cfg, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "DPU host cluster")
}

func TestPrepareWorkDir(t *testing.T) {
	workDir := t.TempDir()
	require.NoError(t, PrepareWorkDir(workDir, DefaultSelector))

	curl := filepath.Join(workDir, "curl-deployment.yml")
	data, err := os.ReadFile(curl)
	require.NoError(t, err)
	require.NotContains(t, string(data), "__NODE_AFFINITY__")
	require.Contains(t, string(data), "operator: Exists")
	require.Contains(t, string(data), config.DPUHostNodeLabelKey)

	job, err := os.ReadFile(filepath.Join(workDir, "node-density-cni.yml"))
	require.NoError(t, err)
	require.Contains(t, string(job), "node-density-cni-dpusim")
}
