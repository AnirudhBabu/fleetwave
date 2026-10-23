package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// RolloutPhase summarises where a rollout is.
// +kubebuilder:validation:Enum=Progressing;Baking;Paused;Succeeded;Halted;RolledBack;Invalid
type RolloutPhase string

const (
	PhaseProgressing RolloutPhase = "Progressing"
	PhaseBaking      RolloutPhase = "Baking"
	PhasePaused      RolloutPhase = "Paused"
	PhaseSucceeded   RolloutPhase = "Succeeded"
	PhaseHalted      RolloutPhase = "Halted"
	PhaseRolledBack  RolloutPhase = "RolledBack"
	PhaseInvalid     RolloutPhase = "Invalid"
)

// Terminal reports whether no further progress happens without a spec change.
func (p RolloutPhase) Terminal() bool {
	return p == PhaseHalted || p == PhaseRolledBack || p == PhaseInvalid
}

// ClusterState is the state of one cluster within a rollout.
// +kubebuilder:validation:Enum=Pending;Progressing;Healthy;Failed;RolledBack;Skipped
type ClusterState string

const (
	ClusterPending     ClusterState = "Pending"
	ClusterProgressing ClusterState = "Progressing"
	ClusterHealthy     ClusterState = "Healthy"
	ClusterFailed      ClusterState = "Failed"
	ClusterRolledBack  ClusterState = "RolledBack"
	ClusterSkipped     ClusterState = "Skipped"
)

// DeletionPolicy decides what happens to resources in member clusters when
// the FleetRollout is deleted.
// +kubebuilder:validation:Enum=Delete;Orphan
type DeletionPolicy string

const (
	DeletionPolicyDelete DeletionPolicy = "Delete"
	DeletionPolicyOrphan DeletionPolicy = "Orphan"
)

// RolloutStrategy controls how fast a revision moves through the fleet.
type RolloutStrategy struct {
	// Waves are cumulative targets, as a cluster count ("1") or a percentage
	// of selected clusters ("25%"). Percentages round up. The last wave is
	// always widened to 100% so a rollout cannot finish half done.
	// +kubebuilder:default={"1","50%","100%"}
	// +kubebuilder:validation:MinItems=1
	// +optional
	Waves []intstr.IntOrString `json:"waves,omitempty"`

	// BakeTime is how long a completed wave must stay healthy before the next
	// wave starts. Clusters that regress during the bake fail the rollout.
	// +kubebuilder:default="60s"
	// +optional
	BakeTime metav1.Duration `json:"bakeTime,omitempty"`

	// ProgressDeadline is how long a cluster may take to become healthy after
	// a revision is applied before it is marked Failed.
	// +kubebuilder:default="5m"
	// +optional
	ProgressDeadline metav1.Duration `json:"progressDeadline,omitempty"`

	// MaxFailures is the number of failed clusters tolerated before the
	// rollout halts. The default of 0 halts on the first failure.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxFailures int32 `json:"maxFailures,omitempty"`

	// AutoRollback re-applies the last revision that succeeded fleet-wide to
	// every cluster that received the failed revision.
	// +kubebuilder:default=true
	// +optional
	AutoRollback *bool `json:"autoRollback,omitempty"`
}

// FleetRolloutSpec is the desired state of a FleetRollout.
type FleetRolloutSpec struct {
	// ClusterSelector selects MemberClusters by label. An empty selector
	// selects every cluster.
	ClusterSelector metav1.LabelSelector `json:"clusterSelector"`

	// Resources are Kubernetes objects applied to every selected cluster with
	// server-side apply. Namespaced objects must set metadata.namespace.
	// +kubebuilder:validation:MinItems=1
	Resources []runtime.RawExtension `json:"resources"`

	// Strategy controls waves, bake time and failure handling.
	// +kubebuilder:default={}
	// +optional
	Strategy RolloutStrategy `json:"strategy,omitempty"`

	// Paused stops new clusters from receiving the revision. Health is still
	// tracked and failures are still acted on.
	// +optional
	Paused bool `json:"paused,omitempty"`

	// DeletionPolicy for resources in member clusters when this object is
	// deleted.
	// +kubebuilder:default=Delete
	// +optional
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`
}

// ClusterRolloutStatus is the state of one cluster.
type ClusterRolloutStatus struct {
	// Name of the MemberCluster.
	Name string `json:"name"`
	// Wave the cluster belongs to (0-based).
	Wave int32 `json:"wave"`
	// Revision currently applied to the cluster by this rollout.
	// +optional
	Revision string `json:"revision,omitempty"`
	// State of the cluster for the desired revision.
	State ClusterState `json:"state"`
	// AppliedAt is when Revision was applied.
	// +optional
	AppliedAt *metav1.Time `json:"appliedAt,omitempty"`
	// Message explains the state.
	// +optional
	Message string `json:"message,omitempty"`
}

// FleetRolloutStatus is the observed state of a FleetRollout.
type FleetRolloutStatus struct {
	// ObservedGeneration is the generation this status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Revision is the content hash of spec.resources being rolled out.
	// +optional
	Revision string `json:"revision,omitempty"`
	// StableRevision is the last revision that reached every selected cluster.
	// It is the rollback target.
	// +optional
	StableRevision string `json:"stableRevision,omitempty"`
	// Phase of the rollout.
	// +optional
	Phase RolloutPhase `json:"phase,omitempty"`
	// CurrentWave is the wave being rolled out (0-based).
	// +optional
	CurrentWave int32 `json:"currentWave"`
	// TotalWaves after resolving percentages against the selected clusters.
	// +optional
	TotalWaves int32 `json:"totalWaves,omitempty"`
	// WaveCompletedAt is when every cluster in CurrentWave became healthy.
	// The next wave starts once BakeTime has passed since then.
	// +optional
	WaveCompletedAt *metav1.Time `json:"waveCompletedAt,omitempty"`
	// Clusters lists per-cluster progress, in wave order.
	// +optional
	Clusters []ClusterRolloutStatus `json:"clusters,omitempty"`
	// Message is a human-readable summary.
	// +optional
	Message string `json:"message,omitempty"`
	// Conditions: Ready, Progressing, Degraded.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// FleetRollout rolls a set of Kubernetes resources across a fleet of member
// clusters in health-gated waves, with automatic rollback.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=fr
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Wave",type=string,JSONPath=`.status.currentWave`
// +kubebuilder:printcolumn:name="Waves",type=string,JSONPath=`.status.totalWaves`
// +kubebuilder:printcolumn:name="Revision",type=string,JSONPath=`.status.revision`
// +kubebuilder:printcolumn:name="Stable",type=string,JSONPath=`.status.stableRevision`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type FleetRollout struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FleetRolloutSpec   `json:"spec,omitempty"`
	Status FleetRolloutStatus `json:"status,omitempty"`
}

// FleetRolloutList contains a list of FleetRollout.
// +kubebuilder:object:root=true
type FleetRolloutList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FleetRollout `json:"items"`
}

func init() {
	SchemeBuilder.Register(&FleetRollout{}, &FleetRolloutList{})
}
