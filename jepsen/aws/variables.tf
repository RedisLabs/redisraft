variable "project" {
  default = "jepsen"
}

variable "nodes_count" {
  default = 5
}

variable "control_user" {
  default = "admin"
}

variable "aws_region" {
  default = "eu-west-1"
}

variable "aws_az" {
  default = "eu-west-1a"
}

variable "aws_instance_type" {
  default = "m5.large"
}

variable "aws_volume_type" {
  default = "gp2"
}

variable "node_volume_size" {
  default = "20"
}

variable "control_volume_size" {
  default = "100"
}

variable "aws_key_pair_name" {
  type = string
  description = "Key pair to use for instances"
}
