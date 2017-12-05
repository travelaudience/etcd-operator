# etcd-operator

This repository was forked from the original
[etcd-operator](https://github.com/coreos/etcd-operator/) repository so that GCS
support could be introduced.

## Pre-Requisites

* Go 1.9+
* Docker 17.09+

## Building

In order to build the Docker image for `etcd-operator` one should clone  this
repository and then run

```
$ hack/build/backup-operator/build && \
    hack/build/backup-trigger/build && \
    hack/build/operator/build && \
    hack/build/restore-operator/build && \
    IMAGE="quay.io/travelaudience/etcd-operator:0.7.0-gcs" hack/build/docker_push
```
