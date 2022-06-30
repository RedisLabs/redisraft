AWS Deployment
==============

This directory contains Terraform and Ansible configuration to assist with
automated deployment on AWS.

The deployment includes N replicas on dedicated EC2 instances, deployed in a
standalone multi-AZ VPC and a single control node from which benchmarks and
other tests can be performed.

Getting Started
---------------

### Prerequisites

1. Terraform: download and install [here](https://www.terraform.io/downloads.html).
2. Ansible: on Ubuntu, use `apt-get install ansible ansible-mitogen`.

### Prepare Terraform configuration

Copy `sample.tfvars` to `myfile.tfvars` and modify the configuration:

* Use an existing EC2 key pair name in `key_pair_name`.
* If `region` is changed, make sure `vpc-azs` are updated accordingly.
* Consult `variables.tf` for more information.

Also, make sure your `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment
variables are set with your AWS credentials.

### Deploy

Run:

    terraform init
    terraform apply -var-file=myfile.tfvars

When completed, the outputs include the public IPs of the control and cluster
nodes.

### Login to control node and run benchmarks

To login to the control node, you will have to have the SSH private key
available and run:

    ssh ubuntu@<control_node_public_addr>

You can now run `memtier_benchmark`. In order to get information about
endpoints, use:

    ./cluster.sh endpoints

You can then run a quick benchmark:

    memtier_benchmark --cluster -s <some endpoint addr> -p <some endpoint port>

### Changing configuration

To change a configuration parameter across all nodes, use:

    ./cluster.sh config set <param> <value>

Tearing down
------------

To tear down the environment, simply run:

    terraform destroy -var-file=myfile.tfvars
