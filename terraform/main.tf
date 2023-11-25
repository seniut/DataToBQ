provider "google" {
  project = var.project_id
  region  = var.region
}


resource "google_composer_environment" "composer_env" {
  provider = google
  name = "airflow-test"
  region = var.region

  config {
    environment_size = "ENVIRONMENT_SIZE_SMALL"

    software_config {
      image_version = "composer-2.5.1-airflow-2.6.3"
    }

    node_config {
      service_account = var.service_account
    }
  }

  storage_config {
    bucket = var.core_dags_folder
  }
}



output "dag_bucket_name" {
  value = google_composer_environment.composer_env.config.0.dag_gcs_prefix

  depends_on = [
    google_composer_environment.composer_env,
  ]
}


#resource "google_storage_bucket" "dag_bucket" {
#  name     = "composer-dags-bucket-${var.project_id}"
#  location = var.region
#}
#
#resource "google_service_account" "composer_service_account" {
#  account_id   = "composer-service-account"
#  display_name = "Composer Service Account"
#}
#
#resource "google_project_iam_member" "composer_service_account_role" {
#  role    = "roles/composer.worker"
#  member  = "serviceAccount:${google_service_account.composer_service_account.email}"
#}
