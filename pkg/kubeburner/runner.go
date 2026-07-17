// Package kubeburner runs kube-burner node-density-cni workloads against a
// dpu-sim host cluster with dpusim.io/vf on every workload pod.
package kubeburner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ovn-kubernetes/dpu-simulator/pkg/config"
	"github.com/ovn-kubernetes/dpu-simulator/pkg/deviceplugin"
	"github.com/ovn-kubernetes/dpu-simulator/pkg/k8s"
	"github.com/ovn-kubernetes/dpu-simulator/pkg/log"
	"github.com/ovn-kubernetes/dpu-simulator/pkg/platform"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DefaultSelector matches DPU-host workers
var DefaultSelector = config.DPUHostNodeLabelKey + "="

// DefaultVFResource is the pseudo-vf resource requested by workload pods.
var DefaultVFResource = deviceplugin.VFResourceName

const (
	// DefaultPodsPerNode is the lab-scale pods-per-node target (2 pods per iteration).
	DefaultPodsPerNode = 45
	// DefaultTimeout is passed to kube-burner init.
	DefaultTimeout = 4 * time.Hour
	// DefaultQPS matches kube-burner-ocp CLI defaults (not client-go's 5/10).
	DefaultQPS = 20
	// DefaultBurst matches kube-burner-ocp CLI defaults.
	DefaultBurst = 20

	namespacePrefix         = "node-density-cni"
	namespaceDeleteTimeout  = 15 * time.Minute
	namespaceDeletePollWait = 2 * time.Second
)

// RunOptions controls how kube-burner is installed and invoked.
type RunOptions struct {
	Kubeconfig    string
	Selector      string
	PodsPerNode   int
	WorkDir       string
	KubeBurnerBin string
	SkipInstall   bool
	SkipPreflight bool
	Force         bool
	NoGC          bool
	QPS           int // 0 = DefaultQPS
	Burst         int // 0 = DefaultBurst (or match QPS if QPS was set)
	Timeout       time.Duration
	VFResource    string
}

// CleanupOptions controls namespace cleanup.
type CleanupOptions struct {
	Kubeconfig string
}

// ComputeIterations returns jobIterations from node count and pods-per-node.
// Each iteration creates one curl + one webserver pod (2 pods), matching
// kube-burner-ocp node-density-cni: (nodes * podsPerNode) / 2.
func ComputeIterations(nodeCount, podsPerNode int) (int, error) {
	if nodeCount <= 0 {
		return 0, fmt.Errorf("node count must be > 0")
	}
	if podsPerNode <= 0 {
		return 0, fmt.Errorf("pods-per-node must be > 0")
	}
	iterations := nodeCount * podsPerNode / 2
	if iterations <= 0 {
		return 0, fmt.Errorf("computed iterations is 0; increase --pods-per-node")
	}
	return iterations, nil
}

// ResolveQPSBurst returns QPS and burst. Defaults match kube-burner-ocp (20 qps/20 burst).
// When qps is unset (<=0), DefaultQPS is used. When burst is unset (<=0), it
// matches the resolved qps (so an explicit --qps without --burst keeps burst=qps).
func ResolveQPSBurst(qps, burst int) (int, int) {
	if qps <= 0 {
		qps = DefaultQPS
	}
	if burst <= 0 {
		burst = qps
	}
	return qps, burst
}

// ResolveKubeconfigPath picks the absolute kubeconfig for the DPU host cluster.
// Explicit kubeconfigOverride wins; otherwise GetDPUHostClusterName() is used.
func ResolveKubeconfigPath(cfg *config.Config, kubeconfigOverride string) (string, error) {
	if kc := strings.TrimSpace(kubeconfigOverride); kc != "" {
		abs, err := filepath.Abs(kc)
		if err != nil {
			return "", fmt.Errorf("kubeconfig path: %w", err)
		}
		if err := k8s.RequireKubeconfigFile(abs, ""); err != nil {
			return "", err
		}
		return abs, nil
	}

	cluster := cfg.GetDPUHostClusterName()
	if cluster == "" {
		return "", fmt.Errorf("no DPU host cluster found in config; kube-burner requires offload_dpu host topology")
	}

	rel := k8s.GetKubeconfigPath(cluster, cfg.Kubernetes.GetKubeconfigDir())
	abs, err := filepath.Abs(rel)
	if err != nil {
		return "", fmt.Errorf("kubeconfig path: %w", err)
	}
	if err := k8s.RequireKubeconfigFile(abs, cluster); err != nil {
		return "", err
	}
	return abs, nil
}

// newK8sClient creates a new Kubernetes client from a kubeconfig file.
func newK8sClient(kubeconfigPath string) (*k8s.K8sClient, error) {
	client, err := k8s.NewClientFromFile(kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}
	return client, nil
}

// Cleanup deletes namespaces matching node-density-cni* without waiting.
func Cleanup(cfg *config.Config, opts CleanupOptions) error {
	if _, err := cfg.GetDeploymentMode(); err != nil {
		return fmt.Errorf("dpu-sim kube-burner needs a valid deployment config (kind or vms): %w", err)
	}

	kc, err := ResolveKubeconfigPath(cfg, opts.Kubeconfig)
	if err != nil {
		return err
	}
	client, err := newK8sClient(kc)
	if err != nil {
		return err
	}
	return cleanupNamespaces(client, false)
}

// Run deletes any leftover node-density-cni namespaces, installs kube-burner
// (unless skipped), prepares templates, runs preflight, and executes kube-burner.
func Run(cmdExec platform.CommandExecutor, cfg *config.Config, opts RunOptions) error {
	if _, err := cfg.GetDeploymentMode(); err != nil {
		return fmt.Errorf("dpu-sim kube-burner needs a valid deployment config (kind: or vms:/baremetal:): %w", err)
	}

	opts = applyDefaults(opts)

	kc, err := ResolveKubeconfigPath(cfg, opts.Kubeconfig)
	if err != nil {
		return err
	}
	opts.Kubeconfig = kc

	client, err := newK8sClient(opts.Kubeconfig)
	if err != nil {
		return err
	}

	// Idempotent re-runs: clear leftovers before installing/creating new objects.
	if err := cleanupNamespaces(client, true); err != nil {
		return fmt.Errorf("pre-run cleanup: %w", err)
	}

	projectRoot, err := platform.GetProjectRoot()
	if err != nil {
		return err
	}

	if opts.WorkDir == "" {
		opts.WorkDir = filepath.Join(projectRoot, DefaultWorkDirName)
	}
	opts.WorkDir, err = filepath.Abs(opts.WorkDir)
	if err != nil {
		return fmt.Errorf("work dir: %w", err)
	}

	bin, err := EnsureBinary(cmdExec, opts.KubeBurnerBin, opts.SkipInstall)
	if err != nil {
		return err
	}
	opts.KubeBurnerBin = bin

	if err := PrepareWorkDir(opts.WorkDir, opts.Selector); err != nil {
		return fmt.Errorf("prepare work dir: %w", err)
	}
	log.Info("Workload templates synced to %s", opts.WorkDir)

	if err := preloadKindImages(cmdExec, cfg); err != nil {
		return err
	}

	podsPerNode := opts.PodsPerNode
	if !opts.SkipPreflight {
		var err error
		podsPerNode, err = preflight(client, opts, podsPerNode)
		if err != nil {
			return err
		}
	}

	nodes, err := client.ListNodes(opts.Selector)
	if err != nil {
		return err
	}
	nodeCount := len(nodes)
	if nodeCount == 0 {
		return fmt.Errorf("no nodes match selector %q", opts.Selector)
	}

	iterations, err := ComputeIterations(nodeCount, podsPerNode)
	if err != nil {
		return err
	}
	qps, burst := ResolveQPSBurst(opts.QPS, opts.Burst)

	userData := filepath.Join(opts.WorkDir, "user-data.yaml")
	if err := writeUserData(userData, iterations, opts.VFResource, qps, burst, !opts.NoGC); err != nil {
		return err
	}

	log.Info("Running node-density-cni-dpusim")
	log.Info("  kubeconfig:   %s", opts.Kubeconfig)
	log.Info("  selector:     %s", opts.Selector)
	log.Info("  nodes:        %d", nodeCount)
	log.Info("  iterations:   %d (pods ≈ %d)", iterations, iterations*2)
	log.Info("  vf resource:  %s", opts.VFResource)
	log.Info("  qps/burst:    %d/%d", qps, burst)
	log.Info("  work dir:     %s", opts.WorkDir)

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	args := []string{
		"init",
		"-c", filepath.Join(opts.WorkDir, "node-density-cni.yml"),
		"--kubeconfig", opts.Kubeconfig,
		"--user-data", userData,
		"--timeout", timeout.String(),
		"--skip-log-file",
	}
	if err := cmdExec.RunCmdInDir(log.LevelInfo, opts.WorkDir, opts.KubeBurnerBin, args...); err != nil {
		log.Error("kube-burner run failed; recent diagnostics:")
		dumpFailureDiagnostics(client)
		return fmt.Errorf("kube-burner: %w", err)
	}

	log.Info("kube-burner run finished")
	if leftover := densityCNIPods(client); len(leftover) > 0 {
		for _, line := range leftover {
			log.Info("%s", line)
		}
	} else {
		log.Info("All node-density-cni pods garbage-collected")
	}
	return nil
}

// applyDefaults applies the default values to the run options.
func applyDefaults(opts RunOptions) RunOptions {
	if opts.Selector == "" {
		opts.Selector = DefaultSelector
	}
	if opts.PodsPerNode <= 0 {
		opts.PodsPerNode = DefaultPodsPerNode
	}
	if opts.VFResource == "" {
		opts.VFResource = DefaultVFResource
	}
	if opts.Timeout <= 0 {
		opts.Timeout = DefaultTimeout
	}
	return opts
}

// preflight checks the preflight conditions that we have enough pseudo-vf resources for
// the number of pods per node.
func preflight(client *k8s.K8sClient, opts RunOptions, podsPerNode int) (int, error) {
	nodes, err := client.ListNodes(opts.Selector)
	if err != nil {
		return podsPerNode, err
	}
	if len(nodes) == 0 {
		return podsPerNode, fmt.Errorf("no nodes match selector %q", opts.Selector)
	}

	minVF := minAllocatableVF(nodes, opts.VFResource)
	if minVF <= 0 {
		return podsPerNode, fmt.Errorf("selected nodes do not advertise %s; is the device plugin running?", opts.VFResource)
	}

	if podsPerNode > minVF && !opts.Force {
		return podsPerNode, fmt.Errorf("--pods-per-node %d exceeds min allocatable %s (%d) per node; re-run with --pods-per-node %d or pass --force",
			podsPerNode, opts.VFResource, minVF, minVF)
	}
	log.Info("Preflight: nodes=%d selector=%q min_%s=%d", len(nodes), opts.Selector, opts.VFResource, minVF)
	return podsPerNode, nil
}

// writeUserData writes the user data for the kube-burner job.
func writeUserData(path string, iterations int, vfResource string, qps, burst int, gc bool) error {
	content := fmt.Sprintf(`JOB_ITERATIONS: %d
VF_RESOURCE: %s
QPS: %d
BURST: %d
GC: %t
GC_METRICS: false
NAMESPACED_ITERATIONS: true
ITERATIONS_PER_NAMESPACE: 1000
CHURN_CYCLES: 0
CHURN_DURATION: 0s
CHURN_PERCENT: 0
CHURN_DELAY: 0s
CHURN_MODE: namespaces
DELETION_STRATEGY: default
POD_READY_THRESHOLD: 0
SVC_LATENCY: false
`, iterations, vfResource, qps, burst, gc)
	return os.WriteFile(path, []byte(content), 0o644)
}

// cleanupNamespaces deletes namespaces under node-density-cni
func cleanupNamespaces(client *k8s.K8sClient, wait bool) error {
	log.Info("Deleting namespaces matching %s*", namespacePrefix)
	namespaces, err := client.GetNamespaces()
	if err != nil {
		return fmt.Errorf("list namespaces: %w", err)
	}

	var toDelete []string
	for _, ns := range namespaces {
		if strings.HasPrefix(ns.Name, namespacePrefix) {
			toDelete = append(toDelete, ns.Name)
		}
	}
	if len(toDelete) == 0 {
		log.Info("No %s* namespaces to clean up", namespacePrefix)
		return nil
	}

	for _, name := range toDelete {
		log.Info("Deleting namespace %s", name)
		if err := client.DeleteNamespace(name); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete namespace %s: %w", name, err)
		}
	}

	if !wait {
		log.Info("Cleanup requested without waiting")
		return nil
	}

	log.Info("Waiting for %s* namespaces to finish deleting", namespacePrefix)
	return waitForNamespacesToDelete(client, toDelete, namespaceDeleteTimeout)
}

// waitForNamespacesToDelete waits for namespaces to delete.
func waitForNamespacesToDelete(client *k8s.K8sClient, names []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	pending := append([]string(nil), names...)
	for len(pending) > 0 {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for namespaces to delete: %s", strings.Join(pending, ", "))
		}
		still := pending[:0]
		for _, name := range pending {
			_, err := client.GetNamespace(name)
			if err == nil {
				still = append(still, name)
				continue
			}
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("check namespace %s: %w", name, err)
			}
		}
		pending = still
		if len(pending) > 0 {
			time.Sleep(namespaceDeletePollWait)
		}
	}
	return nil
}

// minAllocatableVF returns the minimum allocatable VF resource on any node.
func minAllocatableVF(nodes []corev1.Node, vfResource string) int {
	rn := corev1.ResourceName(vfResource)
	min := -1
	for _, node := range nodes {
		qty, ok := node.Status.Allocatable[rn]
		if !ok {
			return 0
		}
		v := int(qty.Value())
		if min < 0 || v < min {
			min = v
		}
	}
	if min < 0 {
		return 0
	}
	return min
}

// dumpFailureDiagnostics dumps failure diagnostics.
func dumpFailureDiagnostics(client *k8s.K8sClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	events, err := client.Clientset().CoreV1().Events("").List(ctx, metav1.ListOptions{})
	if err == nil && len(events.Items) > 0 {
		items := events.Items
		if len(items) > 20 {
			items = items[len(items)-20:]
		}
		for _, ev := range items {
			log.Info("%s %s/%s %s: %s", ev.LastTimestamp.Format(time.RFC3339), ev.Namespace, ev.InvolvedObject.Name, ev.Reason, ev.Message)
		}
	}

	for _, line := range densityCNIPods(client) {
		log.Info("%s", line)
	}
}

// densityCNIPods lists node-density-cni pods.
func densityCNIPods(client *k8s.K8sClient) []string {
	pods, err := client.ListPods("", "")
	if err != nil {
		return nil
	}
	var out []string
	for _, pod := range pods {
		if strings.Contains(pod.Namespace, namespacePrefix) || strings.Contains(pod.Name, namespacePrefix) {
			out = append(out, fmt.Sprintf("%s\t%s\t%s\t%s", pod.Namespace, pod.Name, string(pod.Status.Phase), pod.Spec.NodeName))
		}
	}
	return out
}
