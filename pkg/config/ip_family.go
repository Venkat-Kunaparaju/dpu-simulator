package config

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

const (
	IPFamilyIPv4 = "ipv4"
	IPFamilyIPv6 = "ipv6"
	IPFamilyDual = "dual"
)

func (c *Config) SetDeploymentIPFamily(family string) error {
	if family != IPFamilyIPv4 && family != IPFamilyIPv6 && family != IPFamilyDual {
		return fmt.Errorf("invalid IP family %q: expected ipv4, ipv6 or dual", family)
	}
	for i := range c.Kubernetes.Clusters {
		cluster := &c.Kubernetes.Clusters[i]
		if c.IsOffloadDPU() && c.IsDPUCluster(cluster.Name) {
			continue
		}
		cluster.IPFamily = family
		for _, entry := range []struct {
			cidrs *string
			ipv4  string
			ipv6  string
		}{
			{&cluster.PodCIDR, "10.244.0.0/16", "fd00:10:244::/48"},
			{&cluster.ServiceCIDR, "10.245.0.0/16", "fd00:10:245::/112"},
		} {
			var ipv4, ipv6 string
			for _, cidr := range strings.Split(*entry.cidrs, ",") {
				cidr = strings.TrimSpace(cidr)
				if strings.Contains(cidr, ":") {
					ipv6 = cidr
				} else {
					ipv4 = cidr
				}
			}
			switch family {
			case IPFamilyIPv4:
				*entry.cidrs = ipv4
			case IPFamilyIPv6:
				*entry.cidrs = ipv6
			case IPFamilyDual:
				if ipv4 == "" {
					ipv4 = entry.ipv4
				}
				if ipv6 == "" {
					ipv6 = entry.ipv6
				}
				*entry.cidrs = ipv4 + "," + ipv6
			}
		}
		if err := c.setClusterIPFamilyDefaults(cluster); err != nil {
			return fmt.Errorf("cluster %s: %w", cluster.Name, err)
		}
	}
	return nil
}

func (c *Config) setClusterIPFamilyDefaults(cluster *ClusterConfig) error {
	if cluster.IPFamily == "" {
		cluster.IPFamily = IPFamilyIPv4
	}
	switch cluster.IPFamily {
	case IPFamilyIPv4:
	case IPFamilyIPv6, IPFamilyDual:
		if !c.IsKindMode() || cluster.CNI != CNIOVNKubernetes || (c.IsOffloadDPU() && c.IsDPUCluster(cluster.Name)) || !c.ShouldInstallOVNKubernetes() {
			return fmt.Errorf("IPv6 and dual-stack are supported only for native Kind OVN overlay workload clusters; VM, Flannel, DPU infrastructure and values-only/no-overlay deployments are not supported")
		}
	default:
		return fmt.Errorf("invalid ip_family %q: expected ipv4, ipv6 or dual", cluster.IPFamily)
	}
	if cluster.PodCIDR == "" {
		cluster.PodCIDR = "10.244.0.0/16"
		if cluster.IPFamily == IPFamilyIPv6 {
			cluster.PodCIDR = "fd00:10:244::/48"
		}
		if cluster.IPFamily == IPFamilyDual {
			cluster.PodCIDR += ",fd00:10:244::/48"
		}
	}
	if cluster.ServiceCIDR == "" {
		cluster.ServiceCIDR = "10.245.0.0/16"
		if cluster.IPFamily == IPFamilyIPv6 {
			cluster.ServiceCIDR = "fd00:10:245::/112"
		}
		if cluster.IPFamily == IPFamilyDual {
			cluster.ServiceCIDR += ",fd00:10:245::/112"
		}
	}
	if err := validateFamilyCIDRs(cluster.PodCIDR, cluster.IPFamily, true); err != nil {
		return fmt.Errorf("pod_cidr: %w", err)
	}
	if err := validateFamilyCIDRs(cluster.ServiceCIDR, cluster.IPFamily, false); err != nil {
		return fmt.Errorf("service_cidr: %w", err)
	}
	return nil
}

func validateFamilyCIDRs(value, family string, pod bool) error {
	seen := map[bool]bool{}
	for _, cidr := range strings.Split(value, ",") {
		parts := strings.Split(strings.TrimSpace(cidr), "/")
		if len(parts) < 2 || len(parts) > 3 || (!pod && len(parts) != 2) {
			return fmt.Errorf("invalid CIDR %q", cidr)
		}
		ip, subnet, err := net.ParseCIDR(strings.Join(parts[:2], "/"))
		if err != nil {
			return fmt.Errorf("invalid CIDR %q: %w", cidr, err)
		}
		ipv4 := ip.To4() != nil
		if seen[ipv4] {
			return fmt.Errorf("duplicate address family in %q", value)
		}
		seen[ipv4] = true
		if len(parts) == 3 {
			prefix, err := strconv.Atoi(parts[2])
			clusterPrefix, bits := subnet.Mask.Size()
			if err != nil || prefix <= clusterPrefix || prefix > bits {
				return fmt.Errorf("invalid host subnet prefix in %q: must be greater than %d and no larger than %d", cidr, clusterPrefix, bits)
			}
		}
	}
	wantIPv4 := family != IPFamilyIPv6
	wantIPv6 := family != IPFamilyIPv4
	if seen[true] != wantIPv4 || seen[false] != wantIPv6 {
		return fmt.Errorf("CIDRs %q must match ip_family %s", value, family)
	}
	if family == IPFamilyDual && strings.Contains(strings.Split(value, ",")[0], ":") {
		return fmt.Errorf("CIDRs %q must list IPv4 before IPv6", value)
	}
	return nil
}
