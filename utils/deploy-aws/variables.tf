variable "name" {
  description = "Suffix appended to resource names by default"
  default = "redisraft"
}

variable "region" {
  description = "AWS Region to run in"
}

variable "replicas" {
  description = "Number of instances to set up"
  default = 3
}

variable "vpc-cidr" {
  description = "AWS VPC CIDR block to use"
  default = "10.0.0.0/16"
}

variable "vpc-azs" {
  type = list
  description = "AWS AZs to run in"
}

variable "vpc-subnets" {
  type = list
  description = "Subnet addresses to use"
  default = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "instance_type" {
  description = "AWS EC2 instance type for cluster nodes"
  default = "t3.medium"
}

variable "volume_type" {
  description = "AWS EBS volume type for cluster nodes"
  default = "gp2"
}

variable "volume_size" {
  description = "AWS EBS volume size (GB) for cluster nodes"
  default = "20"
}

variable "control_instance_type" {
  description = "AWS EC2 instance type for control node"
  default = "t3.medium"
}

variable "control_volume_type" {
  description = "AWS EBS volume type for control node"
  default = "gp2"
}

variable "control_volume_size" {
  description = "AWS EBS volume size (GB) for control node"
  default = "20"
}

variable "key_pair_name" {
  type = string
  description = "Key pair to use for instances"
}

variable "user" {
  description = "OS user we use"
  default = "ubuntu"
}

variable "base_port" {
  description = "Base port we use"
  default = 5000
}

variable "shards" {
  description = "Number of shards"
  default = 9
}

