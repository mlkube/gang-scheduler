package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/tensorflow/k8s/cmd/scheduler/k8sclient"
	"github.com/tensorflow/k8s/cmd/scheduler/k8stype"

	"k8s.io/client-go/pkg/api/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sync"
)

var (
	address          string
	batchTimeout     int
	nodeBatchTimeout int
	podChanSize      int
	schedulerName    string
)

func init() {
	flag.StringVar(&address, "addr", "127.0.0.1:8080", "APIServer addr")
	flag.IntVar(&batchTimeout, "pbt", 2, "pods batch timeout in seconds")
	flag.IntVar(&nodeBatchTimeout, "nbt", 2, "node batch timeout in seconds")
	flag.IntVar(&podChanSize, "pcs", 5000, "pod channel size in client's pod informer")
	flag.StringVar(&schedulerName, "scheduler", "default-scheduler", "a name of this scheduler")

	flag.Parse()
}

type tfscheduler struct {
	nodes         []string
	nodeTok8sNode map[string]*v1.Node
	nodesItr      int
	nodeMux       sync.Mutex
	client        *k8sclient.Client
}

func New(client *k8sclient.Client) *tfscheduler {
	return &tfscheduler{
		nodeTok8sNode: make(map[string]*v1.Node),
		client:        client,
	}
}

func main() {
	config := k8sclient.Config{Addr: address, SchedulerName: schedulerName}
	client, err := k8sclient.New(config, podChanSize)
	if err != nil {
		panic(err)
	}

	scheduler := New(client)

	nodes, err := client.GetClientset().CoreV1().Nodes().List(meta_v1.ListOptions{})
	if err != nil {
		panic(err)
	}
	scheduler.nodes = make([]string, 0)
	for _, node := range nodes.Items {
		fmt.Printf("adding a new node: %s\n", node.Name)
		scheduler.nodes = append(scheduler.nodes, node.Name)
	}

	scheduler.Run(client)
}

func (ks *tfscheduler) Run(client *k8sclient.Client) {
	podCh := make(chan *k8stype.Pod)
	ltCh := runLearningTaskMaker(podCh, client.GetClientset())

	go func() {
		for {
			newPods := client.GetPodBatch(time.Duration(batchTimeout) * time.Second)
			for _, pod := range newPods {
				podCh <- pod
			}
		}
	}()

	for {
		lt := <-ltCh
		log.Printf("Adding Pods of learning task %s as tasks to scheduler\n", lt.id)

		podToNodeBindings := make([]*k8stype.Binding, 0)
		for _, pod := range lt.pods {
			podToNodeBindings = append(podToNodeBindings, &k8stype.Binding{pod.ID, ks.nodes[ks.nodesItr], pod.UID})
			fmt.Printf("pod %s -> node %s\n", pod.ID, ks.nodes[ks.nodesItr])
			ks.nodesItr++
			ks.nodesItr %= len(ks.nodes)
		}
		ks.client.AssignBinding(podToNodeBindings)
	}
}

func (ks *tfscheduler) nodeObserve() {
	nch := ks.client.GetNodeChan()
	for {
		select {
		case node := <-nch:
			switch node.Type {
			case k8stype.ADD:
				if _, ok := ks.nodeTok8sNode[node.ID]; ok {
					continue
				}
				log.Printf("Node %s is added.", node.ID)
				ks.nodeTok8sNode[node.ID] = node.NodeInfo
			case k8stype.DELETE:
				if _, ok := ks.nodeTok8sNode[node.ID]; !ok {
					continue
				}
				delete(ks.nodeTok8sNode, node.ID)
			case k8stype.UPDATE:
				ks.nodeTok8sNode[node.ID] = node.NodeInfo
			}
		}
	}
}
