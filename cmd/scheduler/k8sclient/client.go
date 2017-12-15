package k8sclient

import (
	"fmt"
	"path"
	"time"

	"github.com/tensorflow/k8s/cmd/scheduler/k8stype"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api/v1"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

type Config struct {
	Addr          string
	SchedulerName string
}

type Client struct {
	apisrvClient     *kubernetes.Clientset
	unscheduledPodCh chan *k8stype.Pod
	nodeCh           chan *k8stype.Node
}

func checkNodeHealth(node *v1.Node) bool {
	// Skip the master node
	if node.Spec.Unschedulable {
		return false
	}
	// Skip unhealty node
	for _, c := range node.Status.Conditions {
		var desireStatus string
		switch c.Type {
		case "Ready":
			desireStatus = "True"
		default:
			desireStatus = "False"
		}
		if string(c.Status) != desireStatus {
			return false
		}
	}
	for _, t := range node.Spec.Taints {
		if t.Effect == "NoSchedule" {
			return false
		}
	}
	return true
}
func New(cfg Config, podChanSize int) (*Client, error) {
	restCfg := &restclient.Config{
		Host:  fmt.Sprintf("http://%s", cfg.Addr),
		QPS:   1000,
		Burst: 1000,
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, err
	}

	pch := make(chan *k8stype.Pod, podChanSize)

	sel := "spec.nodeName==" + "" + ",status.phase!=" + string(v1.PodSucceeded) + ",status.phase!=" + string(v1.PodFailed)
	informer := cache.NewSharedInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				options.FieldSelector = sel
				return clientset.CoreV1().Pods("").List(options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				options.FieldSelector = sel
				return clientset.CoreV1().Pods("").Watch(options)
			},
		},
		&v1.Pod{},
		0,
	)
	// Add event handlers for the addition, update and deletion of the pods watched by the above informer
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*v1.Pod)
			// fmt.Printf("pod: %v\n", pod)
			// fmt.Printf("pod.Spec.SchedulerName: %s\n", pod.Spec.SchedulerName)
			if pod.Spec.SchedulerName != cfg.SchedulerName {
				return
			}

			ourPod := &k8stype.Pod{
				ID:      makePodID(pod.Namespace, pod.Name),
				UID:     pod.UID,
				PodInfo: pod,
			}

			pch <- ourPod
		},
		UpdateFunc: func(oldObj, newObj interface{}) {},
		DeleteFunc: func(obj interface{}) {
		},
	})
	stopCh := make(chan struct{})
	go informer.Run(stopCh)

	return &Client{
		apisrvClient:     clientset,
		unscheduledPodCh: pch,
	}, nil
}

type PodChan <-chan *k8stype.Pod

func (c *Client) GetUnscheduledPodChan() PodChan {
	return c.unscheduledPodCh
}

type NodeChan <-chan *k8stype.Node

func (c *Client) GetNodeChan() NodeChan {
	return c.nodeCh
}

func (c *Client) AssignBinding(bindings []*k8stype.Binding) error {
	for _, ob := range bindings {
		ns, name := parsePodID(ob.PodID)
		fmt.Printf("binding, namespace: %s, pod name: %s, pod UID: %s, node name: %s\n", ns, name, ob.UID, parseNodeID(ob.NodeID))
		err := c.apisrvClient.CoreV1().Pods(ns).Bind(&v1.Binding{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: ob.UID},
			Target: v1.ObjectReference{
				Kind: "Node",
				Name: parseNodeID(ob.NodeID),
			},
		})
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func (c *Client) GetPodBatch(timeout time.Duration) []*k8stype.Pod {
	batchedPods := make([]*k8stype.Pod, 0)

	fmt.Printf("Waiting for a pod scheduling request\n")

	// Check for first pod, block until at least 1 is available
	pod := <-c.unscheduledPodCh
	batchedPods = append(batchedPods, pod)

	// Set timer for timeout between successive pods
	timer := time.NewTimer(timeout)
	done := make(chan bool)
	go func() {
		<-timer.C
		done <- true
	}()

	fmt.Printf("Batching pod scheduling requests\n")
	numPods := 1
	//fmt.Printf("Number of pods requests: %d", numPods)
	// Poll until done from timeout
	// TODO: Put a cap on the batch size since this could go on forever
	finish := false
	for !finish {
		select {
		case pod = <-c.unscheduledPodCh:
			numPods++
			fmt.Printf("\rNumber of pods requests: %d", numPods)
			batchedPods = append(batchedPods, pod)
			// Refresh the timeout for next pod
			timer.Reset(timeout)
		case <-done:
			finish = true
			fmt.Printf("\n")
		default:
			// Do nothing and keep polling until timeout
		}
	}
	return batchedPods
}

func makePodID(namespace, name string) string {
	return path.Join(namespace, name)
}

func parsePodID(id string) (string, string) {
	ns, podName := path.Split(id)
	// Get rid of the / at the end
	ns = ns[:len(ns)-1]
	return ns, podName
}

func parseNodeID(id string) string {
	return id
}

func (c *Client) GetClientset() *kubernetes.Clientset {
	return c.apisrvClient
}
