# Run Dev Build in Minikube

## Build and Deploy Controller

```bash
# Build the image
cd go
docker build -t kagent-controller:dev .

# Load into minikube
minikube image load kagent-controller:dev

# Set pull policy to Never (only needed once)
kubectl patch deployment kagent-controller -n kagent -p "{\"spec\":{\"template\":{\"spec\":{\"containers\":[{\"name\":\"controller\",\"imagePullPolicy\":\"Never\"}]}}}}"

# Deploy the new image
kubectl set image deployment/kagent-controller controller=kagent-controller:dev -n kagent
```

## After Code Changes

```bash
cd go
docker build -t kagent-controller:dev .
minikube image load kagent-controller:dev
kubectl rollout restart deployment/kagent-controller -n kagent
```
