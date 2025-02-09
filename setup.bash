```bash
#!/bin/bash

# --------------------------- Configuration ---------------------------
PROJECT_ID="your-gcp-project-id"      # Replace with your actual project ID
SERVICE_ACCOUNT_NAME="api-call-worker-sa" # Replace with your desired service account name
SERVICE_ACCOUNT_ID="$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
LOCATION="us-central1" # Replace with your desired project location

# --------------------------- Functions ---------------------------

create_service_account() {
  echo "Checking if service account '$SERVICE_ACCOUNT_ID' exists..."
  if gcloud iam service-accounts describe "$SERVICE_ACCOUNT_ID" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "Service account '$SERVICE_ACCOUNT_ID' already exists. Skipping creation."
  else
    echo "Service account '$SERVICE_ACCOUNT_ID' does not exist. Creating..."
    gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
      --project="$PROJECT_ID" \
      --display-name="Service account for API call worker Cloud Function"
    echo "Service account '$SERVICE_ACCOUNT_ID' created successfully."
  fi
}

grant_roles() {
  echo "Granting necessary roles to service account '$SERVICE_ACCOUNT_ID'..."

  # Cloud Tasks Dequeuer
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SERVICE_ACCOUNT_ID" \
    --role="roles/cloudtasks.dequeuer"

  # Storage Object Viewer
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SERVICE_ACCOUNT_ID" \
    --role="roles/storage.objectViewer"

  # Vertex AI User
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SERVICE_ACCOUNT_ID" \
    --role="roles/aiplatform.user"
    
  # Cloud Function Invoker: If the `api-call-worker` Cloud Function *directly* calls other Cloud Functions
    #gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    #--member="serviceAccount:$SERVICE_ACCOUNT_ID" \
    #--role="roles/cloudfunctions.invoker"
    
  echo "Necessary roles granted to service account '$SERVICE_ACCOUNT_ID'."
}

# --------------------------- Main Script ---------------------------

# Ensure gcloud is authenticated
if ! gcloud auth list --quiet --project="$PROJECT_ID" > /dev/null; then
  echo "ERROR: gcloud is not authenticated. Please run 'gcloud auth login' and try again."
  exit 1
fi

# Check if PROJECT_ID is set
if [ -z "$PROJECT_ID" ]; then
  echo "ERROR: PROJECT_ID is not set. Please set the PROJECT_ID variable."
  exit 1
fi

create_service_account
grant_roles

echo "Script completed successfully."
```
