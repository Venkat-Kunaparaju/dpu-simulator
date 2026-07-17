package kubeburner

import (
	"strings"
	"testing"

	"github.com/ovn-kubernetes/dpu-simulator/pkg/config"

	"github.com/stretchr/testify/require"
)

func TestSelectorToNodeAffinityYAML_Exists(t *testing.T) {
	t.Parallel()
	got, err := SelectorToNodeAffinityYAML(DefaultSelector)
	require.NoError(t, err)
	require.Contains(t, got, "key: "+config.DPUHostNodeLabelKey)
	require.Contains(t, got, "operator: Exists")
	require.False(t, strings.HasSuffix(got, "\n"))
}

func TestSelectorToNodeAffinityYAML_InAndNotIn(t *testing.T) {
	t.Parallel()
	got, err := SelectorToNodeAffinityYAML("role=worker,tier!=control")
	require.NoError(t, err)
	require.Contains(t, got, "operator: In")
	require.Contains(t, got, "- worker")
	require.Contains(t, got, "operator: NotIn")
	require.Contains(t, got, "- control")
}

func TestSelectorToNodeAffinityYAML_DoesNotExist(t *testing.T) {
	t.Parallel()
	got, err := SelectorToNodeAffinityYAML(config.DPUHostNodeLabelKey + "!=")
	require.NoError(t, err)
	require.Contains(t, got, "key: "+config.DPUHostNodeLabelKey)
	require.Contains(t, got, "operator: DoesNotExist")
}

func TestSelectorToNodeAffinityYAML_Errors(t *testing.T) {
	t.Parallel()
	_, err := SelectorToNodeAffinityYAML("")
	require.Error(t, err)
	_, err = SelectorToNodeAffinityYAML("nodash")
	require.Error(t, err)
}
