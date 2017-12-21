package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/tensorflow/k8s/cmd/scheduler/k8stype"
	"github.com/tensorflow/k8s/pkg/spec"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kwatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

type learningTask struct {
	id                 string
	tfjob              *spec.TfJob
	pods               []*k8stype.Pod
	nrRequiredReplicas int
}

type learningTaskMaker struct {
	learningTasks map[string]*learningTask

	podCh      chan *k8stype.Pod
	ltCh       chan *learningTask
	orphanPods map[string][]*k8stype.Pod

	client *kubernetes.Clientset
}

type Event struct {
	Type   kwatch.EventType
	Object *spec.TfJob
}

type rawEvent struct {
	Type   kwatch.EventType
	Object json.RawMessage
}

func pollEvent(decoder *json.Decoder) (*Event, *metav1.Status, error) {
	re := &rawEvent{}
	err := decoder.Decode(re)
	if err != nil {
		if err == io.EOF {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("fail to decode raw event from apiserver (%v)", err)
	}

	if re.Type == kwatch.Error {
		status := &metav1.Status{}
		err = json.Unmarshal(re.Object, status)
		if err != nil {
			return nil, nil, fmt.Errorf("fail to decode (%s) into metav1.Status (%v)", re.Object, err)
		}
		return nil, status, nil
	}

	ev := &Event{
		Type:   re.Type,
		Object: &spec.TfJob{},
	}
	err = json.Unmarshal(re.Object, ev.Object)
	if err != nil {
		return nil, nil, fmt.Errorf("fail to unmarshal Cluster object from data (%s): %v", re.Object, err)
	}
	return ev, nil, nil
}

func listTfJobsURI(ns string) string {
	return fmt.Sprintf("/apis/%s/%s/namespaces/%s/%s", spec.CRDGroup, spec.CRDVersion, ns, spec.CRDKindPlural)
}

func tfJobWatch(jobch chan *spec.TfJob, c *kubernetes.Clientset) {
	restcli := c.CoreV1Client.RESTClient()

	b, err := restcli.Get().RequestURI(listTfJobsURI("default")).DoRaw()
	if err != nil {
		fmt.Printf("failed to list tf jobs: %s\n", err)
		os.Exit(1)
	}

	list := &spec.TfJobList{}
	if err := json.Unmarshal(b, list); err != nil {
		fmt.Printf("failed to list tf jobs: %s\n", err)
		os.Exit(1)
	}

	for i := range list.Items {
		jobch <- &list.Items[i]
	}

	version := list.Metadata.ResourceVersion

	for {
		fmt.Printf("watching version: %s\n", version)
		result := restcli.Get().RequestURI(fmt.Sprintf("/apis/%s/%s/namespaces/default/%s?watch=true&resourceVersion=%s",
			spec.CRDGroup, spec.CRDVersion, spec.CRDKindPlural, version)).Do()
		body, err := result.Raw()
		if err != nil {
			fmt.Printf("failed to watch tfJob: %s\n", err)
			os.Exit(1)
		}

		decoder := json.NewDecoder(bytes.NewReader(body))

		for {
			ev, st, err := pollEvent(decoder)

			if err != nil {
				if err == io.EOF { // apiserver will close stream periodically
					fmt.Printf("apiserver closed stream")
					break
				}

				fmt.Errorf("received invalid event from API server: %v", err)
				os.Exit(1)
			}

			if st != nil {
				fmt.Printf("status: %v\n", st)
				os.Exit(1)
			}

			if ev.Object.Spec.RuntimeId != "" {
				fmt.Printf("added job: %v\n", ev.Object)
				jobch <- ev.Object
			}
			version = ev.Object.Metadata.ResourceVersion
		}
	}
}

func (maker *learningTaskMaker) run() {
	jobCh := make(chan *spec.TfJob)
	go tfJobWatch(jobCh, maker.client)

	for {
		select {
		case pod := <-maker.podCh:
			fmt.Printf("learningTaskMaker: handling newly arrived pod %s\n", pod.PodInfo.Name)
			var lt *learningTask
			var ok bool
			jobId := pod.PodInfo.Labels["runtime_id"]
			fmt.Printf("job id of newly arrived pod: %s\n", jobId)
			if lt, ok = maker.learningTasks[jobId]; !ok {
				// tf job object isn't observed
				if _, ok = maker.orphanPods[jobId]; !ok {
					maker.orphanPods[jobId] = make([]*k8stype.Pod, 0)
				}
				maker.orphanPods[jobId] = append(maker.orphanPods[jobId], pod)
			} else {
				lt.pods = append(lt.pods, pod)

				if len(lt.pods) == lt.nrRequiredReplicas {
					fmt.Printf("lt job %s requires %d pods and all of them are ready, launching\n", jobId, lt.nrRequiredReplicas)
					maker.ltCh <- lt
				}
			}

		case job := <-jobCh:
			jobId := job.Spec.RuntimeId
			fmt.Printf("newly arrived job ID: %s\n", jobId)

			if _, ok := maker.learningTasks[jobId]; ok {
				fmt.Printf("duplicated tf job %s\n", jobId)
				continue
			}

			lt := &learningTask{
				id:    jobId,
				tfjob: job,
				pods:  make([]*k8stype.Pod, 0),
			}
			for _, r := range job.Spec.ReplicaSpecs {
				lt.nrRequiredReplicas += int(*r.Replicas)
			}
			if lt.nrRequiredReplicas == 0 {
				fmt.Printf("invalid nr replicas, skipping %s\n", jobId)
				continue
			}
			fmt.Printf("# of pods belong to the job %s: %d\n", jobId, lt.nrRequiredReplicas)

			maker.learningTasks[jobId] = lt

			if pods, ok := maker.orphanPods[jobId]; ok {
				fmt.Printf("job %s has %d orphan pods: %v\n", jobId, len(pods), pods)
				lt.pods = pods

				if len(lt.pods) == lt.nrRequiredReplicas {
					fmt.Printf("lt job %s requires %d pods and all of them are ready, launching\n", jobId, lt.nrRequiredReplicas)
					maker.ltCh <- lt
				}
			}

		}
	}
}

func runLearningTaskMaker(podCh chan *k8stype.Pod, client *kubernetes.Clientset) chan *learningTask {
	maker := &learningTaskMaker{
		learningTasks: make(map[string]*learningTask),
		podCh:         podCh,
		ltCh:          make(chan *learningTask),
		client:        client,
		orphanPods:    make(map[string][]*k8stype.Pod),
	}

	go maker.run()

	return maker.ltCh
}
