### S3 (MinIO) indexer cronjob 


A cronjob for Kubernetes that lists objects in a S3 (MinIO) bucket


#### Parameters

  * S3_API_HOST
  * S3_API_PORT
  * S3_ACCESS_KEY
  * S3_SECRET_KEY
  * EMBEDDINGS_ENDPOINT


#### Automated deployment

[Manifests repo](https://github.com/vtqveant/manifold-deployment-conf) for ArgoCD.


#### Manual deployment

1. Build an image and push to Harbor (using [Kaniko](https://github.com/GoogleContainerTools/kaniko#running-kaniko-in-a-kubernetes-cluster))

```bash
$ kubectl apply -f kaniko-builder-pod.yaml
```

2. Deploy a cronjob to Kubernetes

```bash
$ kubectl apply -f manifold-indexer-job.yaml
```

3. Check job status

```bash
$ kubectl get cronjob manifold-indexer-job
$ kubectl get jobs --watch
```


### MinIO 

  * [Examples in Python](https://github.com/minio/minio-py)


### Redis

  * [Python client](https://redis.io/docs/clients/python/)
  * [RediSearch](https://github.com/RediSearch/redisearch-getting-started)