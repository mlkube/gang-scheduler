
how to:

1. create your k8s cluster and run kubectl proxy on your local host (assuming kubectl proxy is listening on localhost:8001)
2. execute tf_operator and scheduler like below:

$ ./scheduler -scheduler tf -addr 127.0.0.1:8001
$ export KUBECONFIG=$(echo ~/.kube/config)
$ export MY_POD_NAMESPACE=default
$ export MY_POD_NAME=my-pod
$ KUBERNETES_SERVICE_HOST=localhost KUBERNETES_SERVICE_PORT=8001 ./cmd/tf_operator/tf_operator -scheduler tf # on the top of tensorflow/k8s directory

3. create TfJob like below:
kubectl create -f examples tf_job.yaml
