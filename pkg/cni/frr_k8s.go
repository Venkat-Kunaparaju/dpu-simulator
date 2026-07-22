package cni

import (
	"context"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CreateFRRK8sHostAccess provisions the host-cluster service account token
// that an FRR-K8S daemonset running on the DPU cluster uses as its kubeconfig.
// The FRR-K8S CRDs must exist in the host cluster before the remote daemonset
// starts, but this RBAC is harmless to create earlier.
func (m *CNIManager) CreateFRRK8sHostAccess() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := m.k8sClient.EnsureNamespace(frrK8sNamespace); err != nil {
		return err
	}
	if err := m.k8sClient.EnsureServiceAccount(frrK8sNamespace, "frr-k8s-daemon"); err != nil {
		return err
	}
	if err := m.ensureFRRK8sHostAccessRBAC(ctx); err != nil {
		return err
	}
	if err := m.k8sClient.EnsureServiceAccountTokenSecret(frrK8sNamespace, frrK8sTokenSecretName, "frr-k8s-daemon", 60*time.Second); err != nil {
		return err
	}

	return nil
}

func (m *CNIManager) ensureFRRK8sHostAccessRBAC(ctx context.Context) error {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "frr-k8s-daemon-role", Namespace: frrK8sNamespace},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"secrets"},
				Verbs:     []string{"get", "list", "watch", "update"},
			},
		},
	}
	if err := upsertRole(ctx, m, role); err != nil {
		return err
	}

	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "frr-k8s-daemon-role"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"nodes"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{"admissionregistration.k8s.io"}, Resources: []string{"validatingwebhookconfigurations"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{"admissionregistration.k8s.io"}, Resources: []string{"validatingwebhookconfigurations"}, ResourceNames: []string{"frr-k8s-validating-webhook-configuration"}, Verbs: []string{"update"}},
			{APIGroups: []string{"frrk8s.metallb.io"}, Resources: []string{"frrconfigurations"}, Verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
			{APIGroups: []string{"frrk8s.metallb.io"}, Resources: []string{"frrconfigurations/finalizers"}, Verbs: []string{"update"}},
			{APIGroups: []string{"frrk8s.metallb.io"}, Resources: []string{"frrconfigurations/status"}, Verbs: []string{"get", "patch", "update"}},
			{APIGroups: []string{"frrk8s.metallb.io"}, Resources: []string{"frrnodestates"}, Verbs: []string{"create", "delete", "get", "list", "patch", "update", "watch"}},
			{APIGroups: []string{"frrk8s.metallb.io"}, Resources: []string{"frrnodestates/status"}, Verbs: []string{"get", "patch", "update"}},
		},
	}
	if err := upsertClusterRole(ctx, m, clusterRole); err != nil {
		return err
	}

	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "frr-k8s-daemon-rolebinding", Namespace: frrK8sNamespace},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "frr-k8s-daemon-role"},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "frr-k8s-daemon", Namespace: frrK8sNamespace}},
	}
	if err := upsertRoleBinding(ctx, m, roleBinding); err != nil {
		return err
	}

	clusterRoleBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "frr-k8s-daemon-rolebinding"},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "frr-k8s-daemon-role"},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "frr-k8s-daemon", Namespace: frrK8sNamespace}},
	}
	return upsertClusterRoleBinding(ctx, m, clusterRoleBinding)
}

func upsertRole(ctx context.Context, m *CNIManager, role *rbacv1.Role) error {
	client := m.k8sClient.Clientset().RbacV1().Roles(role.Namespace)
	current, err := client.Get(ctx, role.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = client.Create(ctx, role, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	current.Rules = role.Rules
	_, err = client.Update(ctx, current, metav1.UpdateOptions{})
	return err
}

func upsertClusterRole(ctx context.Context, m *CNIManager, role *rbacv1.ClusterRole) error {
	client := m.k8sClient.Clientset().RbacV1().ClusterRoles()
	current, err := client.Get(ctx, role.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = client.Create(ctx, role, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	current.Rules = role.Rules
	_, err = client.Update(ctx, current, metav1.UpdateOptions{})
	return err
}

func upsertRoleBinding(ctx context.Context, m *CNIManager, binding *rbacv1.RoleBinding) error {
	client := m.k8sClient.Clientset().RbacV1().RoleBindings(binding.Namespace)
	current, err := client.Get(ctx, binding.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = client.Create(ctx, binding, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if current.RoleRef != binding.RoleRef {
		if err := client.Delete(ctx, current.Name, metav1.DeleteOptions{}); err != nil {
			return err
		}
		_, err = client.Create(ctx, binding, metav1.CreateOptions{})
		return err
	}
	current.Subjects = binding.Subjects
	_, err = client.Update(ctx, current, metav1.UpdateOptions{})
	return err
}

func upsertClusterRoleBinding(ctx context.Context, m *CNIManager, binding *rbacv1.ClusterRoleBinding) error {
	client := m.k8sClient.Clientset().RbacV1().ClusterRoleBindings()
	current, err := client.Get(ctx, binding.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = client.Create(ctx, binding, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if current.RoleRef != binding.RoleRef {
		if err := client.Delete(ctx, current.Name, metav1.DeleteOptions{}); err != nil {
			return err
		}
		_, err = client.Create(ctx, binding, metav1.CreateOptions{})
		return err
	}
	current.Subjects = binding.Subjects
	_, err = client.Update(ctx, current, metav1.UpdateOptions{})
	return err
}
