variable "project_id" {
  type        = string
  default     = "datatobq-405917"
  description = "The ID of the project in which resources will be provisioned."
}

variable "region" {
  type        = string
  default     = "us-central1" #"europe-west3"
  description = "The region where Google Cloud resources will be created."
}

variable "service_account" {
  type        = string
  default     = "720840899812-compute@developer.gserviceaccount.com"
  description = "The Service Account"
}

variable "core_dags_folder" {
  type        = string
  default     = "gs://us-central1-airflow-test-5d210d8f-bucket/"
  description = "Dag Folder"
}
