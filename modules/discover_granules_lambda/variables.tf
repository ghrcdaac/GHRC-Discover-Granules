variable "prefix" {
  type = string
  default = null
}

variable "lambda_subnet_ids" {
  type    = list(string)
  default = [""]
}
variable "cumulus_lambda_role" {
  type = string
  default = null
}
variable "lambda_security_group_ids" {
  type    = list(string)
  default = [""]
}
