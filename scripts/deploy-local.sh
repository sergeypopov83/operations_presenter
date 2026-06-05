#!/bin/sh
# RUN THIS SCRIPT IN THE PROJECT ROOT DIRECTORY

# Build your image
sbt assembly  && docker build -t operation-presenter:latest .

# Load into all Kubernetes nodes
for node in desktop-control-plane desktop-worker desktop-worker2; do
  docker save operation-presenter:latest | docker exec -i $node ctr -n k8s.io images import -
done

helm upgrade --install -f operations_presenter/values.yaml -f operations_presenter/values-local.yaml -n  operationspresenter operationspresenter operations_presenter/
