terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0" 
    }
  }
}

provider "google" {
# Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
#  credentials = "path/to/keys.json"
  project = "<Your Project ID>"
  region  = "us-central1"
}



resource "google_storage_bucket" "data-lake-bucket" {
  name          = "<Your Unique Bucket Name>" # this has to be globally unique (can use your unique project Id and append something)
  location      = "US"

  # Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}


resource "google_bigquery_dataset" "dataset" {
  dataset_id = "<The Dataset Name You Want to Use>"
  project    = "<Your Project ID>"
  location   = "US"
}