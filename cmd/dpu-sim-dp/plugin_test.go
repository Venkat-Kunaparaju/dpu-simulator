package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ovn-kubernetes/dpu-simulator/lib/dpusim"
	"github.com/ovn-kubernetes/dpu-simulator/pkg/deviceplugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

// testEnv is the fake world a plugin under test observes: the host netns
// interfaces, kubelet's view of allocated devices, and the clock. Tests
// mutate it between checkDevices calls to simulate runtime events.
type testEnv struct {
	ifaces    []net.Interface
	ifacesErr error
	allocated map[string]bool
	allocErr  error
	now       time.Time
}

func newTestPlugin(t *testing.T, env *testEnv) *DpuSimDevicePlugin {
	t.Helper()
	pools, err := deviceplugin.BuildResourcePools(1)
	require.NoError(t, err)
	require.Len(t, pools, 2)

	p := NewDevicePlugin(pools[1]) // pod VF pool: eth0-2 and up
	p.deviceInfoDir = t.TempDir()
	p.netInterfaces = func() ([]net.Interface, error) { return env.ifaces, env.ifacesErr }
	p.listAllocatedDevices = func(context.Context) (map[string]bool, error) { return env.allocated, env.allocErr }
	p.now = func() time.Time { return env.now }
	require.NoError(t, p.discoverDevices())
	return p
}

func iface(name string, index int) net.Interface {
	return net.Interface{Name: name, Index: index}
}

func healthByID(devices []*pluginapi.Device) map[string]string {
	health := make(map[string]string, len(devices))
	for _, device := range devices {
		health[device.ID] = device.Health
	}
	return health
}

func allocateRequest(ids ...string) *pluginapi.AllocateRequest {
	return &pluginapi.AllocateRequest{
		ContainerRequests: []*pluginapi.ContainerAllocateRequest{{DevicesIds: ids}},
	}
}

var (
	vf2 = dpusim.HostDataIf(2)
	vf3 = dpusim.HostDataIf(3)
)

// TestUnallocatedMissingDeviceIsBenched verifies the core scenario of
// https://github.com/ovn-kubernetes/dpu-simulator/issues/38:
// a netdev destroyed while its device is not allocated to any pod goes
// Unhealthy after unhealthyAfterMisses consecutive checks, and back to
// Healthy when the netdev reappears (even under a different ifindex).
func TestUnallocatedMissingDeviceIsBenched(t *testing.T) {
	t.Parallel()

	env := &testEnv{ifaces: []net.Interface{iface(vf2, 12), iface(vf3, 13)}}
	p := newTestPlugin(t, env)
	ctx := context.Background()

	assert.False(t, p.checkDevices(ctx))

	// vf3's netdev is destroyed. Misses below the threshold keep it Healthy
	// (rides out the CNI DEL move-back window)...
	env.ifaces = []net.Interface{iface(vf2, 12)}
	for i := 1; i < unhealthyAfterMisses; i++ {
		assert.False(t, p.checkDevices(ctx), "miss %d should not bench yet", i)
	}
	assert.Equal(t, pluginapi.Healthy, healthByID(p.devicesSnapshot())[vf3])

	// ...the threshold miss benches it, exactly once.
	assert.True(t, p.checkDevices(ctx))
	assert.Equal(t,
		map[string]string{vf2: pluginapi.Healthy, vf3: pluginapi.Unhealthy},
		healthByID(p.devicesSnapshot()))
	assert.False(t, p.checkDevices(ctx))

	// The netdev reappears, recreated with a new ifindex: Healthy again.
	env.ifaces = []net.Interface{iface(vf2, 12), iface(vf3, 99)}
	assert.True(t, p.checkDevices(ctx))
	assert.Equal(t,
		map[string]string{vf2: pluginapi.Healthy, vf3: pluginapi.Healthy},
		healthByID(p.devicesSnapshot()))
}

// TestAllocatedDeviceAbsentFromHostStaysHealthy verifies that a device whose
// netdev left the host netns (the CNI moved it into a pod) stays Healthy as
// long as kubelet reports it allocated.
func TestAllocatedDeviceAbsentFromHostStaysHealthy(t *testing.T) {
	t.Parallel()

	env := &testEnv{ifaces: []net.Interface{iface(vf2, 12), iface(vf3, 13)}}
	p := newTestPlugin(t, env)
	ctx := context.Background()

	env.ifaces = []net.Interface{iface(vf2, 12)}
	env.allocated = map[string]bool{vf3: true}
	for i := 0; i < unhealthyAfterMisses*3; i++ {
		assert.False(t, p.checkDevices(ctx))
	}
	assert.Equal(t, pluginapi.Healthy, healthByID(p.devicesSnapshot())[vf3])

	// Pod gone (device deallocated) and netdev never came back: benched.
	env.allocated = nil
	for i := 1; i < unhealthyAfterMisses; i++ {
		assert.False(t, p.checkDevices(ctx))
	}
	assert.True(t, p.checkDevices(ctx))
	assert.Equal(t, pluginapi.Unhealthy, healthByID(p.devicesSnapshot())[vf3])
}

// TestRenamedDeviceStaysHealthy verifies that a netdev renamed in the host
// netns (e.g. the mgmt VF becoming ovn-k8s-mp0) is still recognized by its
// ifindex and stays Healthy even when unallocated.
func TestRenamedDeviceStaysHealthy(t *testing.T) {
	t.Parallel()

	env := &testEnv{ifaces: []net.Interface{iface(vf2, 12), iface(vf3, 13)}}
	p := newTestPlugin(t, env)
	ctx := context.Background()

	env.ifaces = []net.Interface{iface(vf2, 12), iface("ovn-k8s-mp0", 13)}
	for i := 0; i < unhealthyAfterMisses*3; i++ {
		assert.False(t, p.checkDevices(ctx))
	}
	// Still two devices (the renamed netdev is not re-added), both Healthy.
	assert.Equal(t,
		map[string]string{vf2: pluginapi.Healthy, vf3: pluginapi.Healthy},
		healthByID(p.devicesSnapshot()))
}

// TestAllocationGracePeriod verifies that a device handed out by Allocate
// counts as in-use before kubelet's PodResources API reflects it, and is
// benched once the grace expires with no allocation and no netdev.
func TestAllocationGracePeriod(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	env := &testEnv{ifaces: []net.Interface{iface(vf2, 12), iface(vf3, 13)}, now: start}
	p := newTestPlugin(t, env)
	ctx := context.Background()

	resp, err := p.Allocate(ctx, allocateRequest(vf3))
	require.NoError(t, err)
	require.Len(t, resp.ContainerResponses, 1)
	assert.Equal(t, vf3, resp.ContainerResponses[0].Envs[p.pool.EnvVarName])

	// Netdev moved into the pod netns; PodResources doesn't list it yet.
	env.ifaces = []net.Interface{iface(vf2, 12)}
	for i := 0; i < unhealthyAfterMisses*3; i++ {
		assert.False(t, p.checkDevices(ctx))
	}
	assert.Equal(t, pluginapi.Healthy, healthByID(p.devicesSnapshot())[vf3])

	// Grace expired, still unallocated per kubelet, netdev never came back.
	env.now = start.Add(allocationGracePeriod + time.Second)
	for i := 1; i < unhealthyAfterMisses; i++ {
		assert.False(t, p.checkDevices(ctx))
	}
	assert.True(t, p.checkDevices(ctx))
	assert.Equal(t, pluginapi.Unhealthy, healthByID(p.devicesSnapshot())[vf3])
}

// TestLateDeviceDiscovery verifies that a pool-matching netdev appearing
// after startup (e.g. a VF that sat inside a pod netns across a plugin
// restart) is added to the advertisement.
func TestLateDeviceDiscovery(t *testing.T) {
	t.Parallel()

	env := &testEnv{ifaces: []net.Interface{iface(vf2, 12)}}
	p := newTestPlugin(t, env)
	ctx := context.Background()

	env.ifaces = []net.Interface{iface(vf2, 12), iface(vf3, 13)}
	assert.True(t, p.checkDevices(ctx))
	assert.Equal(t,
		map[string]string{vf2: pluginapi.Healthy, vf3: pluginapi.Healthy},
		healthByID(p.devicesSnapshot()))

	// Non-matching interfaces are not picked up.
	env.ifaces = append(env.ifaces, iface("docker0", 40), iface(dpusim.HostDataIf(0), 41))
	assert.False(t, p.checkDevices(ctx))
	assert.Len(t, p.devicesSnapshot(), 2)
}

// TestAllocateRejectsUnknownDevice verifies that a device we never
// advertised fails allocation with a clear error.
func TestAllocateRejectsUnknownDevice(t *testing.T) {
	t.Parallel()

	env := &testEnv{ifaces: []net.Interface{iface(vf2, 12), iface(vf3, 13)}}
	p := newTestPlugin(t, env)
	ctx := context.Background()

	_, err := p.Allocate(ctx, allocateRequest("eth0-42"))
	assert.ErrorContains(t, err, "unknown device")

	_, err = p.Allocate(ctx, allocateRequest(vf2))
	assert.NoError(t, err)
}

// TestFailedAllocateGrantsNoGrace verifies that a failed Allocate (device-info
// write error) does not stamp the allocation grace period: kubelet discards
// the allocation on error, so the device must stay bench-able.
func TestFailedAllocateGrantsNoGrace(t *testing.T) {
	t.Parallel()

	env := &testEnv{ifaces: []net.Interface{iface(vf2, 12), iface(vf3, 13)}, now: time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)}
	p := newTestPlugin(t, env)
	ctx := context.Background()

	// Point the device-info dir at a regular file so the write fails.
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o600))
	p.deviceInfoDir = blocker

	_, err := p.Allocate(ctx, allocateRequest(vf3))
	require.Error(t, err)

	// vf3's netdev disappears: with no grace stamped, it is benched after the
	// usual miss threshold instead of surviving on an unearned allocation.
	env.ifaces = []net.Interface{iface(vf2, 12)}
	for i := 1; i < unhealthyAfterMisses; i++ {
		assert.False(t, p.checkDevices(ctx))
	}
	assert.True(t, p.checkDevices(ctx))
	assert.Equal(t, pluginapi.Unhealthy, healthByID(p.devicesSnapshot())[vf3])
}

// TestPodResourcesErrorFailsOpen verifies that when kubelet's PodResources
// API is unavailable, absent devices are not benched (the miss counter is
// frozen), because in-use cannot be told apart from destroyed.
func TestPodResourcesErrorFailsOpen(t *testing.T) {
	t.Parallel()

	env := &testEnv{ifaces: []net.Interface{iface(vf2, 12), iface(vf3, 13)}}
	p := newTestPlugin(t, env)
	ctx := context.Background()

	env.ifaces = []net.Interface{iface(vf2, 12)}
	env.allocErr = context.DeadlineExceeded
	for i := 0; i < unhealthyAfterMisses*3; i++ {
		assert.False(t, p.checkDevices(ctx))
	}
	assert.Equal(t, pluginapi.Healthy, healthByID(p.devicesSnapshot())[vf3])

	// PodResources recovers and confirms vf3 is unallocated: benched.
	env.allocErr = nil
	for i := 1; i < unhealthyAfterMisses; i++ {
		assert.False(t, p.checkDevices(ctx))
	}
	assert.True(t, p.checkDevices(ctx))
	assert.Equal(t, pluginapi.Unhealthy, healthByID(p.devicesSnapshot())[vf3])
}

// TestInterfaceListErrorFailsOpen verifies that when listing host interfaces
// fails, checkDevices reports no change and leaves all health states as they
// were: without a view of the host netns no verdict can be trusted.
func TestInterfaceListErrorFailsOpen(t *testing.T) {
	t.Parallel()

	env := &testEnv{ifaces: []net.Interface{iface(vf2, 12), iface(vf3, 13)}}
	p := newTestPlugin(t, env)
	ctx := context.Background()

	env.ifaces = nil
	env.ifacesErr = context.DeadlineExceeded
	for i := 0; i < unhealthyAfterMisses*3; i++ {
		assert.False(t, p.checkDevices(ctx))
	}
	assert.Equal(t,
		map[string]string{vf2: pluginapi.Healthy, vf3: pluginapi.Healthy},
		healthByID(p.devicesSnapshot()))

	// Listing recovers with vf3 gone: normal benching resumes.
	env.ifacesErr = nil
	env.ifaces = []net.Interface{iface(vf2, 12)}
	for i := 1; i < unhealthyAfterMisses; i++ {
		assert.False(t, p.checkDevices(ctx))
	}
	assert.True(t, p.checkDevices(ctx))
	assert.Equal(t, pluginapi.Unhealthy, healthByID(p.devicesSnapshot())[vf3])
}

// TestEmptyPoolAtStartup verifies that a pool with no matching netdevs at
// startup (e.g. every VF sitting inside a pod netns across a plugin restart,
// or a renamed mgmt VF) advertises an empty device list instead of failing,
// and adopts devices once they appear in the host netns.
func TestEmptyPoolAtStartup(t *testing.T) {
	t.Parallel()

	env := &testEnv{ifaces: []net.Interface{iface(dpusim.HostDataIf(0), 10)}}
	p := newTestPlugin(t, env)
	ctx := context.Background()

	assert.Empty(t, p.devicesSnapshot())
	assert.False(t, p.checkDevices(ctx))

	// A matching netdev appears (e.g. moved back by CNI DEL): adopted.
	env.ifaces = append(env.ifaces, iface(vf2, 12))
	assert.True(t, p.checkDevices(ctx))
	assert.Equal(t,
		map[string]string{vf2: pluginapi.Healthy},
		healthByID(p.devicesSnapshot()))
}

// TestDevicesSnapshotIsIndependent verifies that a snapshot handed to gRPC is
// not mutated by subsequent health transitions.
func TestDevicesSnapshotIsIndependent(t *testing.T) {
	t.Parallel()

	env := &testEnv{ifaces: []net.Interface{iface(vf2, 12)}}
	p := newTestPlugin(t, env)
	ctx := context.Background()

	snapshot := p.devicesSnapshot()

	env.ifaces = nil
	for i := 0; i < unhealthyAfterMisses; i++ {
		p.checkDevices(ctx)
	}
	assert.Equal(t, pluginapi.Healthy, snapshot[0].Health)
	assert.Equal(t, pluginapi.Unhealthy, p.devicesSnapshot()[0].Health)
}
