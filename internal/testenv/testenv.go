// Package testenv starts a hub and several member clusters as separate,
// real kube-apiserver + etcd pairs (controller-runtime envtest), plus a tiny
// "kubelet" simulator that drives Deployment status. It backs the
// integration tests and the demo. Nothing here is used in production.
package testenv

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	fleetv1 "github.com/AnirudhBabu/fleetwave/api/v1alpha1"
)

// SecretNamespace holds member kubeconfigs on the hub.
const SecretNamespace = "fleetwave-system"

// AnnotationUnhealthyIn lists member names where the simulated kubelet
// never makes a Deployment available. It exists only for tests and the demo.
const AnnotationUnhealthyIn = "sim.fleetwave.io/unhealthy-in"

// Scheme knows core types and the fleetwave API.
func Scheme() *k8sruntime.Scheme {
	s := k8sruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(fleetv1.AddToScheme(s))
	return s
}

// Member is one simulated member cluster.
type Member struct {
	Name       string
	Env        *envtest.Environment
	Config     *rest.Config
	Client     client.Client
	Kubeconfig []byte
}

// Fleet is a hub plus members.
type Fleet struct {
	Hub       *envtest.Environment
	HubConfig *rest.Config
	HubClient client.Client
	Members   []*Member

	stopSim context.CancelFunc
	simWG   sync.WaitGroup
}

// RepoRoot returns the repository root, for locating CRDs.
func RepoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

// Start boots the hub (with CRDs) and one API server per member name.
// API servers start in parallel; each takes a few seconds.
func Start(names ...string) (*Fleet, error) {
	f := &Fleet{}
	f.Hub = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(RepoRoot(), "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	f.Members = make([]*Member, len(names))
	errs := make([]error, len(names)+1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cfg, err := f.Hub.Start()
		if err != nil {
			errs[0] = fmt.Errorf("hub: %w", err)
			return
		}
		f.HubConfig = cfg
		f.HubClient, errs[0] = client.New(cfg, client.Options{Scheme: Scheme()})
	}()
	for i, name := range names {
		wg.Add(1)
		go func(i int, name string) {
			defer wg.Done()
			m := &Member{Name: name, Env: &envtest.Environment{}}
			f.Members[i] = m
			cfg, err := m.Env.Start()
			if err != nil {
				errs[i+1] = fmt.Errorf("member %s: %w", name, err)
				return
			}
			m.Config = cfg
			if m.Client, err = client.New(cfg, client.Options{Scheme: Scheme()}); err != nil {
				errs[i+1] = err
				return
			}
			user, err := m.Env.AddUser(envtest.User{Name: "fleetwave-hub", Groups: []string{"system:masters"}}, nil)
			if err != nil {
				errs[i+1] = err
				return
			}
			m.Kubeconfig, errs[i+1] = user.KubeConfig()
		}(i, name)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			f.Stop()
			return nil, err
		}
	}
	ctx := context.Background()
	if err := f.HubClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: SecretNamespace}}); err != nil {
		f.Stop()
		return nil, err
	}
	return f, nil
}

// Member returns a member by name.
func (f *Fleet) Member(name string) *Member {
	for _, m := range f.Members {
		if m.Name == name {
			return m
		}
	}
	return nil
}

// Register stores a member's kubeconfig on the hub and creates its
// MemberCluster, the same way an operator (or Cluster API) would.
func (f *Fleet) Register(ctx context.Context, m *Member, labels map[string]string) error {
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: SecretNamespace, Name: m.Name + "-kubeconfig"},
		Data:       map[string][]byte{"value": m.Kubeconfig},
	}
	if err := f.HubClient.Create(ctx, sec); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	mc := &fleetv1.MemberCluster{
		ObjectMeta: metav1.ObjectMeta{Name: m.Name, Labels: labels},
		Spec: fleetv1.MemberClusterSpec{KubeconfigSecretRef: fleetv1.SecretKeyReference{
			Namespace: SecretNamespace, Name: sec.Name, Key: "value",
		}},
	}
	return f.HubClient.Create(ctx, mc)
}

// StartKubeletSim makes each member behave like a cluster with working
// nodes: Deployments become available shortly after they change, unless the
// Deployment's AnnotationUnhealthyIn names that member, in which case its
// pods "crash-loop" and it never becomes available.
func (f *Fleet) StartKubeletSim(interval time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	f.stopSim = cancel
	for _, m := range f.Members {
		f.simWG.Add(1)
		go func(m *Member) {
			defer f.simWG.Done()
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					simulate(ctx, m)
				}
			}
		}(m)
	}
}

func simulate(ctx context.Context, m *Member) {
	var deps appsv1.DeploymentList
	if err := m.Client.List(ctx, &deps); err != nil {
		return
	}
	for i := range deps.Items {
		d := &deps.Items[i]
		want := int32(1)
		if d.Spec.Replicas != nil {
			want = *d.Spec.Replicas
		}
		broken := false
		for _, n := range strings.Split(d.Annotations[AnnotationUnhealthyIn], ",") {
			if strings.TrimSpace(n) == m.Name {
				broken = true
			}
		}
		st := appsv1.DeploymentStatus{
			ObservedGeneration: d.Generation,
			Replicas:           want,
			UpdatedReplicas:    want,
			ReadyReplicas:      want,
			AvailableReplicas:  want,
		}
		if broken {
			st.ReadyReplicas, st.AvailableReplicas, st.UnavailableReplicas = 0, 0, want
		}
		if d.Status.ObservedGeneration == st.ObservedGeneration && d.Status.AvailableReplicas == st.AvailableReplicas &&
			d.Status.UpdatedReplicas == st.UpdatedReplicas && d.Status.Replicas == st.Replicas {
			continue
		}
		d.Status = st
		_ = m.Client.Status().Update(ctx, d)
	}
}

// Stop tears everything down.
func (f *Fleet) Stop() {
	if f.stopSim != nil {
		f.stopSim()
		f.simWG.Wait()
	}
	for _, m := range f.Members {
		if m != nil && m.Env != nil {
			_ = m.Env.Stop()
		}
	}
	if f.Hub != nil {
		_ = f.Hub.Stop()
	}
}
