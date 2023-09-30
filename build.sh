#!/bin/bash

REGISTRY=harbor.eventflow.ru/library
IMAGE=manifold-indexer-job
TAG=`git rev-parse --short HEAD`

docker build . -t ${REGISTRY}/${IMAGE}:${TAG} && \
    docker tag ${REGISTRY}/${IMAGE}:${TAG} ${REGISTRY}/${IMAGE}:latest && \
    docker push ${REGISTRY}/${IMAGE}:${TAG} && \
    docker push ${REGISTRY}/${IMAGE}:latest