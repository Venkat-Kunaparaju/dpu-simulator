package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/ovn-kubernetes/dpu-simulator/pkg/config"
	"github.com/ovn-kubernetes/dpu-simulator/pkg/kubeburner"
	"github.com/ovn-kubernetes/dpu-simulator/pkg/log"
	"github.com/ovn-kubernetes/dpu-simulator/pkg/platform"
	"github.com/spf13/cobra"
)

var (
	kbKubeconfig    string
	kbSelector      string
	kbPodsPerNode   int
	kbWorkDir       string
	kbBin           string
	kbSkipInstall   bool
	kbSkipPreflight bool
	kbForce         bool
	kbNoGC          bool
	kbQPS           int
	kbBurst         int
	kbTimeout       time.Duration
	kbVFResource    string
)

var kubeBurnerCmd = &cobra.Command{
	Use:   "kube-burner",
	Short: "kube-burner node-density-cni for DPU-host offload",
	Long: `Run kube-burner node-density-cni against a dpu-sim host cluster with
dpusim.io/vf on every workload pod.

Uses embedded workload templates from pkg/kubeburner/templates
and always targets the DPU host cluster kubeconfig from --config.

In Kind mode, sampleapp/curl are pulled and kind-loaded into the host
cluster before init (kube-burner preLoadImages stays off: its preload DS
cannot request dpusim.io/vf).

If kube-burner is not on PATH, the official install script is used:
  curl -Ls https://raw.githubusercontent.com/kube-burner/kube-burner/refs/heads/main/hack/install.sh | bash

Examples:
  ./bin/dpu-sim kube-burner run --config config-kind-ovnk-offload.yaml
  ./bin/dpu-sim kube-burner run --pods-per-node 5 --skip-install --kube-burner-bin /usr/local/bin/kube-burner
  ./bin/dpu-sim kube-burner cleanup --config config-kind-ovnk-offload.yaml`,
}

var kubeBurnerRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Clean leftover namespaces, then run kube-burner init",
	RunE:  runKubeBurner,
}

var kubeBurnerCleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Delete node-density-cni-* namespaces",
	RunE:  cleanupKubeBurner,
}

func init() {
	kubeBurnerCmd.PersistentFlags().StringVar(&kbKubeconfig, "kubeconfig", "",
		"DPU host cluster kubeconfig (default: kubeconfig/<dpu-host-cluster>.kubeconfig from --config)")
	kubeBurnerRunCmd.Flags().StringVar(&kbSelector, "selector", kubeburner.DefaultSelector,
		"Node label selector for DPU-host workers")

	kubeBurnerRunCmd.Flags().IntVar(&kbPodsPerNode, "pods-per-node", kubeburner.DefaultPodsPerNode,
		"Pods per selected node (kube-burner-ocp style; job iterations = nodes * pods-per-node / 2)")
	kubeBurnerRunCmd.Flags().StringVar(&kbWorkDir, "work-dir", "",
		"Working directory for rendered templates (default: workloads/kube-burner-node-density-cni-dpusim)")
	kubeBurnerRunCmd.Flags().StringVar(&kbBin, "kube-burner-bin", "",
		"Path to kube-burner binary (default: PATH or ~/.local/bin/kube-burner)")
	kubeBurnerRunCmd.Flags().BoolVar(&kbSkipInstall, "skip-install", false,
		"Do not run the official kube-burner install script")
	kubeBurnerRunCmd.Flags().BoolVar(&kbSkipPreflight, "skip-preflight", false,
		"Skip VF capacity / node selector checks")
	kubeBurnerRunCmd.Flags().BoolVar(&kbForce, "force", false,
		"Run even if pods-per-node exceeds allocatable dpusim.io/vf")
	kubeBurnerRunCmd.Flags().BoolVar(&kbNoGC, "no-gc", false,
		"Leave namespaces after the run (default: garbage-collect)")
	kubeBurnerRunCmd.Flags().IntVar(&kbQPS, "qps", kubeburner.DefaultQPS,
		"kube-burner API QPS (kube-burner-ocp default)")
	kubeBurnerRunCmd.Flags().IntVar(&kbBurst, "burst", kubeburner.DefaultBurst,
		"kube-burner API burst (kube-burner-ocp default)")
	kubeBurnerRunCmd.Flags().DurationVar(&kbTimeout, "timeout", kubeburner.DefaultTimeout,
		"Timeout passed to kube-burner init")
	kubeBurnerRunCmd.Flags().StringVar(&kbVFResource, "vf-resource", kubeburner.DefaultVFResource,
		"Pseudo-VF Resource name requested by workload pods")

	kubeBurnerCmd.AddCommand(kubeBurnerRunCmd)
	kubeBurnerCmd.AddCommand(kubeBurnerCleanupCmd)
	rootCmd.AddCommand(kubeBurnerCmd)
}

func runKubeBurner(_ *cobra.Command, args []string) error {
	log.SetLevel(log.ParseLevel(logLevel))
	if len(args) > 0 {
		return fmt.Errorf("unexpected arguments: %v", args)
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	return kubeburner.Run(platform.NewLocalExecutor(), cfg, kubeburner.RunOptions{
		Kubeconfig:    strings.TrimSpace(kbKubeconfig),
		Selector:      kbSelector,
		PodsPerNode:   kbPodsPerNode,
		WorkDir:       kbWorkDir,
		KubeBurnerBin: kbBin,
		SkipInstall:   kbSkipInstall,
		SkipPreflight: kbSkipPreflight,
		Force:         kbForce,
		NoGC:          kbNoGC,
		QPS:           kbQPS,
		Burst:         kbBurst,
		Timeout:       kbTimeout,
		VFResource:    kbVFResource,
	})
}

func cleanupKubeBurner(_ *cobra.Command, args []string) error {
	log.SetLevel(log.ParseLevel(logLevel))
	if len(args) > 0 {
		return fmt.Errorf("unexpected arguments: %v", args)
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	return kubeburner.Cleanup(cfg, kubeburner.CleanupOptions{
		Kubeconfig: strings.TrimSpace(kbKubeconfig),
	})
}
