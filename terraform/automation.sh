#!/bin/bash

# Function to deploy resources
deploy() {
    terraform init
    terraform plan
    terraform apply -auto-approve

    DAG_BUCKET=$(terraform output -raw dag_bucket_name)
    echo "bucket: $DAG_BUCKET"

    gsutil cp -r ../dags/* "${DAG_BUCKET}"/
    echo "Deployment complete, and DAGs are uploaded."
}

# Function to destroy resources
destroy() {
    terraform destroy -auto-approve
    echo "Resources destroyed."
}

# Check script argument for deploy or destroy
if [ "$1" == "deploy" ]; then
    deploy
elif [ "$1" == "destroy" ]; then
    destroy
else
    echo "Usage: $0 [deploy|destroy]"
fi
