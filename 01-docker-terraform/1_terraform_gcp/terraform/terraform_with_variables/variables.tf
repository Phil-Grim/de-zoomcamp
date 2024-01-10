locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "credentials" {
  description = "My Credentials"
  default     = "/home/USERNAME/.gc/evident-display-410312-6d17d29a1ecf.json"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}


variable "project" { 
  description = "Project"
  default     = "evident-display-410312"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "europe-west2"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "London"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "trips_data_all"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "terraform-demo-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "TABLE_NAME" {
  description = "BigQuery Table"
  type = string
  default = "ny_trips"
}