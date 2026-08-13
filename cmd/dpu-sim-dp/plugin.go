// Package main implements a simulated Kubernetes device plugin that exposes
// host-to-DPU data interfaces as allocatable resources. Multiple resource
// pools are supported — each pool gets its own gRPC socket and kubelet
// registration so that OVN-Kubernetes DPU-host mode can allocate management-
// port and pod VFs independently through the standard device plugin mechanism.
//
// Pools are built from pkg/deviceplugin.BuildResourcePools.
//
// Unlike a real PCI VF, a simulated VF is a veth and can be destroyed at
// runtime (e.g. by pod network namespace teardown). Advertised devices are
// therefore health-checked continuously; see checkDevices for the rules that
// distinguish a destroyed netdev from one legitimately in use by a pod.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ovn-kubernetes/dpu-simulator/pkg/deviceplugin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"
)

const (
	dpDeviceInfoPath = "/var/run/k8s.cni.cncf.io/devinfo/dp"

	// podResourcesSocket is kubelet's PodResources gRPC endpoint, used by the
	// health checker to learn which devices are allocated to running pods.
	podResourcesSocket = "/var/lib/kubelet/pod-resources/kubelet.sock"

	// healthCheckInterval is how often advertised devices are re-checked
	// against the host netns and kubelet's allocations.
	healthCheckInterval = 5 * time.Second

	// unhealthyAfterMisses is how many consecutive health checks must find a
	// device gone (not present and not allocated) before it is reported
	// Unhealthy. Waiting several checks avoids benching a healthy device
	// during pod teardown, when kubelet has already freed the allocation but
	// the CNI has not yet moved the netdev back to the host netns.
	unhealthyAfterMisses = 3

	// allocationGracePeriod is how long after Allocate a device counts as
	// in-use even if kubelet's PodResources API does not list it yet.
	allocationGracePeriod = time.Minute
)

// deviceState tracks one advertised device beyond what the device plugin API
// carries: the netdev ifindex (to keep recognizing the device after a rename,
// e.g. ovnkube-node renames its mgmt VF to ovn-k8s-mp0), when it was last
// handed out by Allocate, and how many consecutive health checks missed it.
type deviceState struct {
	device            *pluginapi.Device
	ifindex           int
	lastAllocated     time.Time
	consecutiveMisses int
}

// DpuSimDevicePlugin implements the Kubernetes device plugin API for a single resource pool.
type DpuSimDevicePlugin struct {
	pluginapi.UnimplementedDevicePluginServer
	pool          deviceplugin.ResourcePool
	socketPath    string
	deviceInfoDir string
	server        *grpc.Server

	// netInterfaces, listAllocatedDevices and now are set to real
	// implementations by NewDevicePlugin; tests override them.
	netInterfaces        func() ([]net.Interface, error)
	listAllocatedDevices func(ctx context.Context) (map[string]bool, error)
	now                  func() time.Time

	podResOnce sync.Once
	podResConn *grpc.ClientConn
	podResErr  error

	mu      sync.Mutex // guards devices (slice and deviceState contents)
	devices []*deviceState
}

func NewDevicePlugin(pool deviceplugin.ResourcePool) *DpuSimDevicePlugin {
	p := &DpuSimDevicePlugin{
		pool:          pool,
		socketPath:    filepath.Join(pluginapi.DevicePluginPath, pool.SocketName),
		deviceInfoDir: dpDeviceInfoPath,
		netInterfaces: net.Interfaces,
		now:           time.Now,
	}
	p.listAllocatedDevices = p.kubeletAllocatedDevices
	return p
}

func (p *DpuSimDevicePlugin) discoverDevices() error {
	ifaces, err := p.netInterfaces()
	if err != nil {
		return fmt.Errorf("failed to list interfaces: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.devices = nil
	for _, iface := range ifaces {
		if p.pool.MatchesIface(iface.Name) {
			p.devices = append(p.devices, &deviceState{
				device:  &pluginapi.Device{ID: iface.Name, Health: pluginapi.Healthy},
				ifindex: iface.Index,
			})
			klog.Infof("[%s] discovered device: %s (ifindex=%d)", p.pool.ResourceName, iface.Name, iface.Index)
		}
	}

	if len(p.devices) == 0 {
		return fmt.Errorf("[%s] no interfaces matching %s found", p.pool.ResourceName, p.pool.MatcherDescription())
	}
	klog.Infof("[%s] discovered %d device(s)", p.pool.ResourceName, len(p.devices))
	return nil
}

// Run discovers devices, starts the gRPC server, and registers with kubelet.
// It blocks until ctx is cancelled.
func (p *DpuSimDevicePlugin) Run(ctx context.Context) error {
	if err := p.discoverDevices(); err != nil {
		return err
	}

	// TODO: Reclaim stale device-info files only after reconciling live
	// allocations, so files still referenced by active pods are preserved.
	if err := p.writeDeviceInfoFiles(); err != nil {
		return err
	}

	// Reconcile health once before advertising, so a device that vanished
	// between discovery and registration is not reported Healthy to kubelet.
	p.checkDevices(ctx)

	if err := p.startServer(); err != nil {
		return fmt.Errorf("[%s] failed to start gRPC server: %w", p.pool.ResourceName, err)
	}

	if err := p.registerWithKubelet(); err != nil {
		return fmt.Errorf("[%s] failed to register with kubelet: %w", p.pool.ResourceName, err)
	}

	klog.Infof("[%s] registered with kubelet (devices=%d)", p.pool.ResourceName, len(p.devicesSnapshot()))

	<-ctx.Done()
	p.server.GracefulStop()
	if p.podResConn != nil {
		p.podResConn.Close()
	}
	os.Remove(p.socketPath)
	return nil
}

func (p *DpuSimDevicePlugin) startServer() error {
	os.Remove(p.socketPath)

	listener, err := net.Listen("unix", p.socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", p.socketPath, err)
	}

	p.server = grpc.NewServer()
	pluginapi.RegisterDevicePluginServer(p.server, p)

	go func() {
		if err := p.server.Serve(listener); err != nil {
			klog.Errorf("gRPC server exited: %v", err)
		}
	}()

	// Wait for the socket to become ready.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "unix://"+p.socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("gRPC server did not become ready: %w", err)
	}
	conn.Close()
	return nil
}

func (p *DpuSimDevicePlugin) registerWithKubelet() error {
	conn, err := grpc.Dial("unix://"+pluginapi.KubeletSocket,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to kubelet: %w", err)
	}
	defer conn.Close()

	client := pluginapi.NewRegistrationClient(conn)
	_, err = client.Register(context.Background(), &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     p.pool.SocketName,
		ResourceName: p.pool.ResourceName,
	})
	return err
}

func (p *DpuSimDevicePlugin) GetDevicePluginOptions(context.Context, *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{}, nil
}

func (p *DpuSimDevicePlugin) ListAndWatch(_ *pluginapi.Empty, stream pluginapi.DevicePlugin_ListAndWatchServer) error {
	if err := stream.Send(&pluginapi.ListAndWatchResponse{Devices: p.devicesSnapshot()}); err != nil {
		return err
	}

	// Re-check device health periodically and notify kubelet on transitions.
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			if p.checkDevices(stream.Context()) {
				if err := stream.Send(&pluginapi.ListAndWatchResponse{Devices: p.devicesSnapshot()}); err != nil {
					return err
				}
			}
		}
	}
}

// checkDevices reconciles advertised devices with the host netns and
// kubelet's allocations, and reports whether the advertisement changed.
//
// Absence from the host netns alone does not mean a device is gone: the CNI
// moves an allocated VF into the pod's netns for the pod's lifetime, and
// ovnkube-node renames its mgmt VF (eth0-N -> ovn-k8s-mp0). A device is
// reported Unhealthy only when it is not allocated to any pod (kubelet's
// PodResources API, plus a grace period after Allocate), not present in the
// host netns by name or by ifindex, and that has persisted for
// unhealthyAfterMisses consecutive checks. It flips back to Healthy as soon
// as it is present or in use again.
func (p *DpuSimDevicePlugin) checkDevices(ctx context.Context) bool {
	ifaces, err := p.netInterfaces()
	if err != nil {
		klog.Errorf("[%s] health check: failed to list interfaces: %v", p.pool.ResourceName, err)
		return false
	}

	allocated, allocErr := p.listAllocatedDevices(ctx)
	if allocErr != nil {
		klog.Errorf("[%s] health check: failed to list kubelet pod resources: %v", p.pool.ResourceName, allocErr)
	}

	byName := make(map[string]net.Interface, len(ifaces))
	byIndex := make(map[int]bool, len(ifaces))
	for _, iface := range ifaces {
		byName[iface.Name] = iface
		byIndex[iface.Index] = true
	}

	now := p.now()
	changed := false

	p.mu.Lock()
	for _, s := range p.devices {
		present := false
		if iface, ok := byName[s.device.ID]; ok {
			present = true
			s.ifindex = iface.Index
		} else if s.ifindex != 0 && byIndex[s.ifindex] {
			// Renamed but still alive in the host netns.
			present = true
		}
		inUse := allocated[s.device.ID] ||
			(!s.lastAllocated.IsZero() && now.Sub(s.lastAllocated) < allocationGracePeriod)

		switch {
		case present || inUse:
			s.consecutiveMisses = 0
			if s.device.Health != pluginapi.Healthy {
				klog.Infof("[%s] device %s is back (present=%v, inUse=%v), marking Healthy", p.pool.ResourceName, s.device.ID, present, inUse)
				s.device.Health = pluginapi.Healthy
				changed = true
			}
		case allocErr != nil:
			// Without allocation data we cannot tell in-use from destroyed;
			// fail open and freeze the miss counter.
		default:
			s.consecutiveMisses++
			if s.consecutiveMisses >= unhealthyAfterMisses && s.device.Health != pluginapi.Unhealthy {
				klog.Warningf("[%s] device %s: netdev gone and not allocated to any pod for %d checks, marking Unhealthy", p.pool.ResourceName, s.device.ID, s.consecutiveMisses)
				s.device.Health = pluginapi.Unhealthy
				changed = true
			}
		}
	}
	p.mu.Unlock()
	return changed
}

// kubeletAllocatedDevices returns the IDs of this pool's devices currently
// allocated to pods, according to kubelet's PodResources API.
func (p *DpuSimDevicePlugin) kubeletAllocatedDevices(ctx context.Context) (map[string]bool, error) {
	p.podResOnce.Do(func() {
		p.podResConn, p.podResErr = grpc.NewClient("unix://"+podResourcesSocket,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
	})
	if p.podResErr != nil {
		return nil, fmt.Errorf("failed to create PodResources client: %w", p.podResErr)
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := podresourcesapi.NewPodResourcesListerClient(p.podResConn).List(ctx, &podresourcesapi.ListPodResourcesRequest{})
	if err != nil {
		return nil, fmt.Errorf("PodResources List failed: %w", err)
	}

	allocated := make(map[string]bool)
	for _, pod := range resp.GetPodResources() {
		for _, container := range pod.GetContainers() {
			for _, dev := range container.GetDevices() {
				if dev.GetResourceName() != p.pool.ResourceName {
					continue
				}
				for _, id := range dev.GetDeviceIds() {
					allocated[id] = true
				}
			}
		}
	}
	return allocated, nil
}

func (p *DpuSimDevicePlugin) Allocate(_ context.Context, req *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	resp := &pluginapi.AllocateResponse{}
	for _, creq := range req.ContainerRequests {
		ids := strings.Join(creq.DevicesIds, ",")
		klog.Infof("[%s] allocating devices: %s", p.pool.ResourceName, ids)
		if err := p.validateDevices(creq.DevicesIds); err != nil {
			return nil, err
		}
		for _, id := range creq.DevicesIds {
			if err := p.writeDeviceInfoFile(id); err != nil {
				return nil, err
			}
		}
		// Mark allocated only once everything else succeeded: on error kubelet
		// discards the allocation, so the devices must not carry an unearned
		// in-use grace period.
		p.markAllocated(creq.DevicesIds)
		resp.ContainerResponses = append(resp.ContainerResponses, &pluginapi.ContainerAllocateResponse{
			Envs: map[string]string{
				p.pool.EnvVarName: ids,
			},
		})
	}
	return resp, nil
}

// validateDevices rejects allocation requests for devices we do not advertise.
func (p *DpuSimDevicePlugin) validateDevices(ids []string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	known := make(map[string]bool, len(p.devices))
	for _, s := range p.devices {
		known[s.device.ID] = true
	}
	for _, id := range ids {
		if !known[id] {
			return fmt.Errorf("[%s] refusing to allocate unknown device %s", p.pool.ResourceName, id)
		}
	}
	return nil
}

// markAllocated records the allocation time of the requested devices so the
// health checker tolerates the netdev moving into the pod's netns before
// kubelet's PodResources API reflects the allocation.
func (p *DpuSimDevicePlugin) markAllocated(ids []string) {
	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()

	byID := make(map[string]*deviceState, len(p.devices))
	for _, s := range p.devices {
		byID[s.device.ID] = s
	}
	for _, id := range ids {
		if s, ok := byID[id]; ok {
			s.lastAllocated = now
			s.consecutiveMisses = 0
		}
	}
}

// devicesSnapshot returns a copy of the device list safe to hand to gRPC
// while the health checker keeps mutating the originals.
func (p *DpuSimDevicePlugin) devicesSnapshot() []*pluginapi.Device {
	p.mu.Lock()
	defer p.mu.Unlock()
	devices := make([]*pluginapi.Device, 0, len(p.devices))
	for _, s := range p.devices {
		devices = append(devices, &pluginapi.Device{
			ID:       s.device.ID,
			Health:   s.device.Health,
			Topology: s.device.Topology,
		})
	}
	return devices
}

func (p *DpuSimDevicePlugin) writeDeviceInfoFiles() error {
	for _, device := range p.devicesSnapshot() {
		if err := p.writeDeviceInfoFile(device.ID); err != nil {
			return err
		}
	}
	return nil
}

func (p *DpuSimDevicePlugin) writeDeviceInfoFile(deviceID string) error {
	if err := os.MkdirAll(p.deviceInfoDir, 0755); err != nil {
		return fmt.Errorf("[%s] failed to create device info directory: %w", p.pool.ResourceName, err)
	}

	info := map[string]string{
		"version": "1.1.0",
	}
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return fmt.Errorf("[%s] failed to marshal device info for %s: %w", p.pool.ResourceName, deviceID, err)
	}
	data = append(data, '\n')

	path := filepath.Join(p.deviceInfoDir, deviceInfoFileName(p.pool.ResourceName, deviceID))
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("[%s] failed to write device info for %s: %w", p.pool.ResourceName, deviceID, err)
	}
	klog.Infof("[%s] wrote device info for %s to %s", p.pool.ResourceName, deviceID, path)
	return nil
}

func deviceInfoFileName(resourceName, deviceID string) string {
	return fmt.Sprintf("%s-%s-device.json",
		strings.ReplaceAll(resourceName, "/", "-"),
		strings.ReplaceAll(deviceID, "/", "-"))
}

func (p *DpuSimDevicePlugin) GetPreferredAllocation(context.Context, *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	return &pluginapi.PreferredAllocationResponse{}, nil
}

func (p *DpuSimDevicePlugin) PreStartContainer(context.Context, *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}
