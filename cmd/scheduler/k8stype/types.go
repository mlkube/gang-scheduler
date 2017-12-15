package k8stype

import (
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/pkg/api/v1"
)

type MessageType int

const (
	ADD MessageType = iota
	DELETE
	UPDATE
)

type Pod struct {
	ID      string
	UID     types.UID
	PodInfo *v1.Pod
}

type Node struct {
	ID       string
	NodeInfo *v1.Node
	Type     MessageType
}

type Binding struct {
	PodID  string
	NodeID string
	UID    types.UID
}
