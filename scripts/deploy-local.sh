#!/bin/sh
# RUN THIS SCRIPT IN THE PROJECT ROOT DIRECTORY

# Build your image
sbt assembly  && docker build -t operationpresenter:latest .

# Load into all Kubernetes nodes
for node in desktop-control-plane desktop-worker desktop-worker2; do
  docker save operationpresenter:latest | docker exec -i $node ctr -n k8s.io images import -
done

helm upgrade --install -f operationspresenter/values.yaml -f operationspresenter/values-local.yaml -n  operationspresenter operationspresenter operationspresenter/
