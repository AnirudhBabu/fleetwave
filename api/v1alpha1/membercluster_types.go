package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Well-known labels and condition types.
const (
	// LabelCanary marks a cluster that should always land in the first wave.
	LabelCanary = "fleet.fleetwave.io/canary"

	// ConditionReady reports whether the hub can reach the member cluster's API server.
	ConditionReady = "Ready"
)

// SecretKeyReference points at one key of a Secret holding a kubeconfig.
type SecretKeyReference struct {
	// Namespace of the Secret.
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`

	// Name of the Secret.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Key inside the Secret. Defaults to "value", which is the key Cluster API
	// uses for its <cluster>-kubeconfig Secrets, so CAPI-provisioned clusters
	// can be registered without copying credentials around.
	// +kubebuilder:default=value
	// +optional
	Key string `json:"key,omitempty"`
}

// MemberClusterSpec defines how the hub reaches a member cluster.
type MemberClusterSpec struct {
	// KubeconfigSecretRef references the kubeconfig used to talk to the cluster.
	KubeconfigSecretRef SecretKeyReference `json:"kubeconfigSecretRef"`

	// Cordoned excludes the cluster from new rollout waves. Clusters that are
	// already on a revision keep it. Use this to take a broken or
	// maintenance-bound cluster out of the path of a rollout instead of
	// letting it block every wave.
	// +optional
	Cordoned bool `json:"cordoned,omitempty"`
}

// MemberClusterStatus is the observed state of a member cluster.
type MemberClusterStatus struct {
	// KubernetesVersion reported by the member's API server.
	// +optional
	KubernetesVersion string `json:"kubernetesVersion,omitempty"`

	// LastProbeTime is when the hub last tried to reach the cluster.
	// +optional
	LastProbeTime *metav1.Time `json:"lastProbeTime,omitempty"`

	// Conditions: Ready.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// MemberCluster registers a Kubernetes cluster with the fleetwave hub.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=mc
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.status.kubernetesVersion`
// +kubebuilder:printcolumn:name="Cordoned",type=boolean,JSONPath=`.spec.cordoned`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type MemberCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MemberClusterSpec   `json:"spec,omitempty"`
	Status MemberClusterStatus `json:"status,omitempty"`
}

// MemberClusterList contains a list of MemberCluster.
// +kubebuilder:object:root=true
type MemberClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MemberCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MemberCluster{}, &MemberClusterList{})
}
