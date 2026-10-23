package controller

import (
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CacheOptions narrows the hub's informer cache: only ControllerRevisions
// fleetwave owns are cached, not every StatefulSet's and DaemonSet's history
// on the hub. Secrets are only watched as metadata (see MemberClusterReconciler).
func CacheOptions() cache.Options {
	req, err := labels.NewRequirement(LabelRolloutName, selection.Exists, nil)
	if err != nil {
		panic(err)
	}
	return cache.Options{ByObject: map[client.Object]cache.ByObject{
		&appsv1.ControllerRevision{}: {Label: labels.NewSelector().Add(*req)},
	}}
}
