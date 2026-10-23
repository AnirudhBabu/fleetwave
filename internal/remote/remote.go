// Package remote builds and caches clients for member clusters.
//
// Clients are uncached (no informers): with 160+ clusters, an informer per
// kind per cluster in the hub process would cost far more memory and watch
// connections than the occasional GETs a rollout needs. The trade-off is
// written up in DESIGN.md.
package remote

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	fleetv1 "github.com/AnirudhBabu/fleetwave/api/v1alpha1"
)

// Remote is a connection to one member cluster.
type Remote struct {
	Client    client.Client
	Discovery discovery.DiscoveryInterface
}

// Getter returns a Remote for a MemberCluster.
type Getter interface {
	Get(ctx context.Context, mc *fleetv1.MemberCluster) (*Remote, error)
}

// Cache builds Remotes from kubeconfig Secrets and reuses them until the
// Secret changes (keyed on resourceVersion), so credential rotation is
// picked up without a restart.
type Cache struct {
	// Reader must be uncached: the hub should not hold every Secret in
	// memory just to read a few kubeconfigs.
	Reader  client.Reader
	Timeout time.Duration
	QPS     float32
	Burst   int

	mu      sync.Mutex
	entries map[string]entry
}

type entry struct {
	secretVersion string
	remote        *Remote
}

// Get implements Getter.
func (c *Cache) Get(ctx context.Context, mc *fleetv1.MemberCluster) (*Remote, error) {
	ref := mc.Spec.KubeconfigSecretRef
	key := ref.Key
	if key == "" {
		key = "value"
	}
	var s corev1.Secret
	if err := c.Reader.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, &s); err != nil {
		return nil, fmt.Errorf("reading kubeconfig secret %s/%s: %w", ref.Namespace, ref.Name, err)
	}
	version := s.ResourceVersion + "/" + key

	c.mu.Lock()
	if c.entries == nil {
		c.entries = map[string]entry{}
	}
	if e, ok := c.entries[mc.Name]; ok && e.secretVersion == version {
		c.mu.Unlock()
		return e.remote, nil
	}
	c.mu.Unlock()

	data, ok := s.Data[key]
	if !ok || len(data) == 0 {
		return nil, fmt.Errorf("secret %s/%s has no key %q", ref.Namespace, ref.Name, key)
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig(data)
	if err != nil {
		return nil, fmt.Errorf("parsing kubeconfig for %s: %w", mc.Name, err)
	}
	r, err := c.build(cfg)
	if err != nil {
		return nil, fmt.Errorf("building client for %s: %w", mc.Name, err)
	}

	c.mu.Lock()
	c.entries[mc.Name] = entry{secretVersion: version, remote: r}
	c.mu.Unlock()
	return r, nil
}

// Forget drops a cluster's client, e.g. when the MemberCluster is deleted.
func (c *Cache) Forget(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, name)
}

func (c *Cache) build(cfg *rest.Config) (*Remote, error) {
	cfg = rest.CopyConfig(cfg)
	cfg.Timeout = c.Timeout
	if cfg.Timeout == 0 {
		cfg.Timeout = 15 * time.Second
	}
	if c.QPS > 0 {
		cfg.QPS = c.QPS
	}
	if c.Burst > 0 {
		cfg.Burst = c.Burst
	}
	cfg.UserAgent = "fleetwave"
	cl, err := client.New(cfg, client.Options{})
	if err != nil {
		return nil, err
	}
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Remote{Client: cl, Discovery: dc}, nil
}
