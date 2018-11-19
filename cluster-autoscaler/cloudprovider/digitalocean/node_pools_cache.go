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

	"github.com/digitalocean/godo"
)

var nodePoolCacheMissErr = errors.New("node pool was not in cache")

type nodePoolsCache struct {
	client *godo.Client

	// map of node pool names to godo.KubernetesNodePool
	nodePools map[string]*godo.KubernetesNodePool

	clusterID string
}

func newNodePoolsCache(client *godo.Client, clusterID string) *nodePoolsCache {
	return &nodePoolsCache{
		client:    client,
		nodePools: make(map[string]*godo.KubernetesNodePool, 0),
		clusterID: clusterID,
	}
}

func (n *nodePoolsCache) get(nodePoolName string) (*godo.KubernetesNodePool, error) {
	nodePool, ok := n.nodePools[nodePoolName]
	if !ok {
		return nil, nodePoolCacheMissErr
	}

	return nodePool, nil
}

func (n *nodePoolsCache) refresh() error {
	nodePools, _, err := n.client.Kubernetes.ListNodePools(context.TODO(), n.clusterID, &godo.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list Kubernetes node pools: %s", err)
	}

	newNodePools := map[string]*godo.KubernetesNodePool{}
	for _, n := range nodePools {
		newNodePools[n.Name] = n
	}

	n.nodePools = newNodePools
	return nil
}
