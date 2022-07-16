gcloud container clusters get-credentials kubefun --region us-central1 --project $GOOGLE_CLOUD_PROJECT;
export DAGIT_POD_NAME=$(kubectl get pods --namespace default -l "app.kubernetes.io/name=dagster,app.kubernetes.io/instance=dagster,component=dagit" -o jsonpath="{.items[0].metadata.name}")
kubectl --namespace default port-forward $DAGIT_POD_NAME 8080:80;