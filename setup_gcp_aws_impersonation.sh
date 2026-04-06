#!/bin/bash

# Configuration with minimal user input
echo "Starting GCP-AWS Workload Identity Setup..."

read -p "Enter GCP Project ID: " PROJECT_ID
read -p "Enter AWS Account ID: " AWS_ACCOUNT_ID
read -p "Enter AWS IAM Role Name: " AWS_ROLE_NAME

# Setup project configuration
gcloud config set project "$PROJECT_ID"
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")

echo "Enabling necessary APIs..."
gcloud services enable iam.googleapis.com sts.googleapis.com iamcredentials.googleapis.com cloudresourcemanager.googleapis.com bigquery.googleapis.com bigquerystorage.googleapis.com

echo "Creating GCP Service Account: data-collector..."
gcloud iam service-accounts create data-collector --display-name="Data Collector" || true

echo "Setting up Workload Identity Pool: aws-pool..."
gcloud iam workload-identity-pools create "aws-pool" \
    --location="global" \
    --display-name="AWS Identity Pool" || true

echo "Setting up AWS Provider: aws-provider..."
gcloud iam workload-identity-pools providers create-aws "aws-provider" \
    --workload-identity-pool="aws-pool" \
    --location="global" \
    --account-id="$AWS_ACCOUNT_ID" \
    --attribute-mapping="google.subject=assertion.arn,attribute.aws_account=assertion.account,attribute.aws_role=assertion.arn.extract('assumed-role/{role_name}/')" || true

echo "Granting IAM permissions to AWS principal..."
MEMBER="principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/aws-pool/attribute.aws_role/$AWS_ROLE_NAME"

gcloud iam service-accounts add-iam-policy-binding "data-collector@$PROJECT_ID.iam.gserviceaccount.com" \
    --member="$MEMBER" \
    --role="roles/iam.workloadIdentityUser"

gcloud iam service-accounts add-iam-policy-binding "data-collector@$PROJECT_ID.iam.gserviceaccount.com" \
    --member="$MEMBER" \
    --role="roles/iam.serviceAccountTokenCreator"

echo "Granting BigQuery permissions to service account..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:data-collector@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:data-collector@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:data-collector@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataViewer"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:data-collector@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.readSessionUser"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:data-collector@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.user"

echo "Generating creds.json..."
gcloud iam workload-identity-pools create-cred-config \
    "projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/aws-pool/providers/aws-provider" \
    --service-account="data-collector@$PROJECT_ID.iam.gserviceaccount.com" \
    --output-file="creds.json" \
    --aws

echo "Done! Configuration saved to creds.json"
