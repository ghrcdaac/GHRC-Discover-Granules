variable "aws_profile" {
  type    = string
  default = "SBX"
}

variable "cumulus_lambda_role_arn" {
  type = string
}

variable "cumulus_lambda_role_name" {
  type = string
}

variable "env_variables" {
  type    = map(string)
  default = {}
}

variable "minimum_acu" {
  default = 2
}

variable "maximum_acu" {
  default = 16
}

variable "security_group_ids" {
  type    = list(string)
  default = null
}

variable "subnet_ids" {
  type    = list(string)
  default = null
}

variable "layers" {
  type    = list(string)
  default = []
}

variable "memory_size" {
  description = "Lambda RAM limit"
  default     = 2048
}

variable "ignore_discovered" {
  description = "On the initial run in a workflow any record that matches the provider path prefix and the collection ID and a status of \"discovered\" will be updated to \"ignored\""
  default = false
}

variable "prefix" {
  type = string
}

variable "region" {
  type    = string
  default = "us-west-2"
}

variable "s3_bucket_name" {
  type = string
}

variable "timeout" {
  description = "Lambda function time-out"
  default     = 900
}

variable "early_return_threshold" {
  description = "If the remaining execution time of the lambda goes below this an early return will be performed."
  default = 30
}

# Sqlite Configuration
variable "sqlite_transaction_size" {
  type = number
  default = 100000
}

variable "sqlite_temp_store" {
  type = number
  default = 0
}

variable "sqlite_cache_size" {
  type = number
  default = (-1 * 64000)
}

# RDS Configuration
variable "deprovision_v1" {
  type = bool
  default = false
}

variable "perform_switchover" {
  type = bool
  default = false
}

variable "db_identifier" {
  type = string
  default = "gdgdb"
}

variable "db_instance_class" {
  type = string
  default = "db.t3.micro"
}

variable "db_username" {
  type = string
  default = "gdgdbadmin"
}

variable "db_allocated_storage" {
  type = number
  default = 5
}

variable "db_type" {
  type = string

  validation {
    condition = contains(["postgresql", "sqlite", "cumulus"], var.db_type)
    error_message = "The variable db_type must be one of: postgresql, sqlite, or cumulus."
  }
}

variable "cumulus_user_credentials_secret_arn" {
  type = string
  default = null
}
