### S3 (MinIO) indexer cronjob 


A cronjob for Kubernetes that lists objects in a S3 (MinIO) bucket


1. Build an image and push to Harbor

```bash
$ docker build . -t manifold-indexer-job
$ docker tag manifold-indexer-job harbor.eventflow.ru/library/manifold-indexer-job:latest
$ docker push harbor.eventflow.ru/library/manifold-indexer-job:latest
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
