# CI/CD Pipeline for Dask Job Deployment

## Overview

This document outlines the CI/CD pipeline process for deploying Dask jobs to an EKS cluster. The process involves several steps, from making changes to the Dask job code to executing the job in a Kubernetes cluster.

## Steps

### 1. Git Commit
- **Developer makes changes to the Dask job code**: The developer edits the Dask job code in their local development environment.
- **Changes are committed and pushed to a Git repository**: The developer commits their changes with a meaningful message and pushes the commit to the remote Git repository.

### 2. CI/CD Pipeline Trigger
- **CI/CD system detects the new commit**: A CI/CD system like Jenkins, GitLab CI, or GitHub Actions monitors the repository for changes.
- **The pipeline is triggered automatically**: When a new commit is detected, the CI/CD pipeline is automatically triggered.

### 3. Build Process
- **The CI/CD system pulls the latest code**: The CI/CD system checks out the latest code from the repository.
- **Builds a Docker image containing the Dask environment and job code**: The CI/CD system uses a Dockerfile to build a Docker image that includes the necessary Dask environment and the job code.


### 4. Image Push
- **The built Docker image is tagged with a version or commit hash**: The Docker image is tagged with a unique identifier, such as a version number or commit hash.
- **Image is pushed to a container registry (e.g., Amazon ECR)**: The tagged Docker image is pushed to a container registry.

### 5. Kubernetes Manifest Update
- **CI/CD system updates Kubernetes manifests (e.g., Job template)**: The CI/CD system updates the Kubernetes manifests to reference the new Docker image.
- **Updates image tag in the manifests to match the new build**: The image tag in the Kubernetes manifests is updated to the new version or commit hash.

### 6. EKS Cluster Deployment
- **CI/CD system connects to the EKS cluster using kubectl**: The CI/CD system uses kubectl to connect to the EKS cluster.
- **Applies updated Kubernetes manifests to the cluster**: The updated Kubernetes manifests are applied to the EKS cluster.

### 7. Job Submission
- **CI/CD system or a separate process triggers job submission**: The CI/CD system or a separate process submits the job to the Kubernetes cluster.
- **Creates a Kubernetes Job based on the updated template**: A Kubernetes Job is created using the updated job template.

### 8. EKS Scheduling
- **Kubernetes scheduler in EKS assigns the job to a node**: The Kubernetes scheduler assigns the job to a suitable node in the cluster.
- **Pulls the specified Docker image from the registry**: The node pulls the specified Docker image from the container registry.

### 9. Job Execution
- **Job pod starts and runs the Dask script**: The job pod starts and executes the Dask script.
- **Dask client in the job connects to the existing Dask cluster**: The Dask client within the job connects to the existing Dask cluster for execution.

### 10. Result Handling
- **Job completes and stores results (e.g., in S3 or a database)**: Upon completion, the job stores its results in a predefined location like S3 or a database.
- **Job status is updated in Kubernetes**: The job status is updated in Kubernetes to reflect its completion status.

### 11. Monitoring and Logging
- **Logs are collected from the job pod**: Logs from the job pod are collected in AWS Cloudwatch
- **Metrics are gathered from the Dask dashboard**: Metrics from the Dask dashboard are gathered for performance monitoring.

