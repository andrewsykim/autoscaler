/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package digitalocean

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	kerrors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"

	"github.com/digitalocean/godo"
	"github.com/golang/glog"

	"golang.org/x/oauth2"
)

const (
	// ProviderName is the cloud provider name for DigitalOcean
	ProviderName      string = "digitalocean"
	doAccessTokenEnv  string = "DO_ACCESS_TOKEN"
	doK8sClusterIDEnv string = "DO_CLUSTER_ID"
)

type tokenSource struct {
	AccessToken string
}

func (t *tokenSource) Token() (*oauth2.Token, error) {
	token := &oauth2.Token{
		AccessToken: t.AccessToken,
	}
	return token, nil
}

type DOCloudProvider struct {
	client *godo.Client

	clusterID string

	// a cache of all node pools matching the cluster ID
	nodePoolsCache *nodePoolsCache

	// instead of node Pools maybe we just need nodeGroups cached in this struct
	nodeGroups []*nodeGroup
	// node group specs passed in from config.AutoscalingOptions
	// expected format is min:max:node-pool-id
	nodeGroupSpec []nodeGroupSpec

	resourceLimiter *cloudprovider.ResourceLimiter
}

type nodeGroupSpec struct {
	min int
	max int
	id  string
}

func BuildDigitalOcean(opts config.AutoscalingOptions, do cloudprovider.NodeGroupDiscoveryOptions, rl *cloudprovider.ResourceLimiter) cloudprovider.CloudProvider {
	clusterID := os.Getenv(doK8sClusterIDEnv)
	if clusterID == "" {
		glog.Fatalf("%s env var is required", doK8sClusterIDEnv)
	}

	token := os.Getenv(doAccessTokenEnv)
	if token == "" {
		glog.Fatalf("%s env var is required", doAccessTokenEnv)
	}

	tokenSource := &tokenSource{
		AccessToken: token,
	}

	oauthClient := oauth2.NewClient(oauth2.NoContext, tokenSource)
	client := godo.NewClient(oauthClient)

	nodePoolsCache := newNodePoolsCache(client, clusterID)
	nodeGroupSpec, err := parseNodeGroupSpec(opts.NodeGroups)
	if err != nil {
		glog.Fatalf("error parsing node groups: %s", err)
	}

	provider := &DOCloudProvider{
		client:          client,
		clusterID:       clusterID,
		nodePoolsCache:  nodePoolsCache,
		nodeGroupSpec:   nodeGroupSpec,
		nodeGroups:      make([]*nodeGroup, 0),
		resourceLimiter: rl,
	}

	return provider
}

func parseNodeGroupSpec(specs []string) ([]nodeGroupSpec, error) {
	nodeGroupSpecs := make([]nodeGroupSpec, 0)
	for _, spec := range specs {
		split := strings.Split(spec, ":")
		if len(split) != 3 {
			return nil, fmt.Errorf("unexpected format for node spec: %s", spec)
		}

		min, err := strconv.Atoi(split[0])
		if err != nil {
			return nil, fmt.Errorf("failed to convert min string to int: %s", err)
		}

		max, err := strconv.Atoi(split[1])
		if err != nil {
			return nil, fmt.Errorf("failed to convert min string to int: %s", err)
		}

		id := split[2]

		nodeGroupSpec := nodeGroupSpec{
			min: min,
			max: max,
			id:  id,
		}
		nodeGroupSpecs = append(nodeGroupSpecs, nodeGroupSpec)
	}

	return nodeGroupSpecs, nil
}

func (d *DOCloudProvider) Name() string {
	return ProviderName
}

func (d *DOCloudProvider) NodeGroups() []cloudprovider.NodeGroup {
	nodeGroups := make([]cloudprovider.NodeGroup, 0)

	for _, ngSpec := range d.nodeGroupSpec {
		k8sNodePool, err := d.nodePoolsCache.get(ngSpec.id)
		if err == nodePoolCacheMissErr {
			glog.Errorf("could not get node pool with ID %s, err: %s", ngSpec.id, err)
			continue
		}
		nodeGroup := &nodeGroup{
			client:    d.client,
			clusterID: d.clusterID,
			id:        ngSpec.id,
			min:       ngSpec.min,
			max:       ngSpec.max,
			nodePool:  k8sNodePool,
		}

		nodeGroups = append(nodeGroups, nodeGroup)
	}

	return nodeGroups
}

func (d *DOCloudProvider) NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	// providerID := node.Spec.ProviderID
	// if providerID == "" {
	//	return nil, fmt.Errorf("node %s is not registered with the cloud provider", node.Name)
	// }

	// dropletID, err := dropletIDFromProviderID(providerID)
	// if err != nil {
	//		return nil, fmt.Errorf("error getting droplet ID from provider ID of node %s", node.Name)
	//}

	for _, ngSpec := range d.nodeGroupSpec {
		k8sNodePool, err := d.nodePoolsCache.get(ngSpec.id)
		if err == nodePoolCacheMissErr {
			glog.Errorf("could not get node pool with ID %s, err: %s", ngSpec.id, err)
			continue
		}

		for _, k8sNode := range k8sNodePool.Nodes {
			if k8sNode.Name == node.Name {
				return &nodeGroup{
					client:    d.client,
					clusterID: d.clusterID,
					id:        ngSpec.id,
					min:       ngSpec.min,
					max:       ngSpec.max,
					nodePool:  k8sNodePool,
				}, nil
			}
		}
	}

	return nil, fmt.Errorf("could not get node group for node %s", node.Name)
}

func (d *DOCloudProvider) Pricing() (cloudprovider.PricingModel, kerrors.AutoscalerError) {
	return nil, cloudprovider.ErrNotImplemented
}

func (d *DOCloudProvider) GetAvailableMachineTypes() ([]string, error) {
	return []string{}, nil
}

func (d *DOCloudProvider) NewNodeGroup(machineType string, labels map[string]string, systemLabels map[string]string,
	taints []apiv1.Taint, extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (d *DOCloudProvider) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	return d.resourceLimiter, nil
}

func (d *DOCloudProvider) Cleanup() error {
	return nil
}

func (d *DOCloudProvider) Refresh() error {
	return d.nodePoolsCache.refresh()
}

type nodeGroup struct {
	client *godo.Client

	name string
	min  int
	max  int

	clusterID string
	nodePool  *godo.KubernetesNodePool
}

func (n *nodeGroup) MaxSize() int {
	return n.max
}

func (n *nodeGroup) MinSize() int {
	return n.min
}

func (n *nodeGroup) TargetSize() (int, error) {
	return n.nodePool.Count, nil
}

func (n *nodeGroup) IncreaseSize(delta int) error {
	newCount := n.nodePool.Count + delta

	req := &godo.KubernetesNodePoolUpdateRequest{
		Name:  n.nodePool.Name,
		Count: newCount,
		Tags:  n.nodePool.Tags,
	}

	_, _, err := n.client.Kubernetes.UpdateNodePool(context.TODO(), n.clusterID, n.id, req)
	return err
}

// TODO: once decreasing target size does not delete nodes on backend
func (n *nodeGroup) DeleteNodes([]*apiv1.Node) error {
	return nil
}

func (n *nodeGroup) DecreaseTargetSize(delta int) error {
	newCount := n.nodePool.Count + delta

	req := &godo.KubernetesNodePoolUpdateRequest{
		Name:  n.nodePool.Name,
		Count: newCount,
		Tags:  n.nodePool.Tags,
	}

	_, _, err := n.client.Kubernetes.UpdateNodePool(context.TODO(), n.clusterID, n.id, req)
	return err
}

func (n *nodeGroup) Id() string {
	return n.id
}

func (n *nodeGroup) Debug() string {
	return ""
}

func (n *nodeGroup) Nodes() ([]cloudprovider.Instance, error) {
	instances := make([]cloudprovider.Instance, 0)
	for _, node := range n.nodePool.Nodes {
		// TODO: update InstanceStatus as well
		instance := cloudprovider.Instance{
			// apparently this needs to be provider ID
			Id: node.Name,
		}

		instances = append(instances, instance)
	}

	return instances, nil
}

func (n *nodeGroup) TemplateNodeInfo() (*schedulercache.NodeInfo, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (n *nodeGroup) Exist() bool {
	return true
}

func (n *nodeGroup) Create() (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (n *nodeGroup) Delete() error {
	return cloudprovider.ErrNotImplemented
}

func (n *nodeGroup) Autoprovisioned() bool {
	return false
}

// dropletIDFromProviderID returns a droplet's ID from providerID.
//
// The providerID spec should be retrievable from the Kubernetes
// node object. The expected format is: digitalocean://droplet-id
func dropletIDFromProviderID(providerID string) (string, error) {
	if providerID == "" {
		return "", errors.New("providerID cannot be empty string")
	}

	split := strings.Split(providerID, "/")
	if len(split) != 3 {
		return "", fmt.Errorf("unexpected providerID format: %s, format should be: digitalocean://12345", providerID)
	}

	// since split[0] is actually "digitalocean:"
	if strings.TrimSuffix(split[0], ":") != ProviderName {
		return "", fmt.Errorf("provider name from providerID should be digitalocean: %s", providerID)
	}

	return split[2], nil
}
