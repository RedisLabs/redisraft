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
2. Ansible: on recent Ubuntu, simply use:

    apt-get install ansible ansible-mitogen

### Prepare Terraform configuration

Copy `sample.tfvars` to `myfile.tfvars` and modify the configuration:

* Use an existing EC2 key pair name in `key_pair_name`.
* If `region` is changed, make sure `vpc-azs` are updated accordingly.
* Consult `variables.tf` for more information.

Also, make sure your `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment
variables are set with your AWS credentials.

### Prepare Ansible configuration

Run `./create_instances.sh` to create a `instances.yml` file that describes the
shards (shard groups), hash slots and ports. For example:

    ./create_instances.sh --shards 9 --nodes 3 --base-port 5000

### Create AWS infrastructure with Terraform

Run:

    terraform init
    terraform apply -var-file=myfile.tfvars

When completed, the outputs include the public IPs of the control and cluster
nodes.

In addition, the Ansible inventory file `ansible/inventory/hosts.cfg` is
generated.

### Install and configure everything

Next, run Ansible to install and configure the nodes:

    ansible-playbook --extra-vars "@instances.yml" ansible/site.yml

It is also possible to deploy a custom version of RedisRaft:

    ansible-playbook \
        --extra-vars redisraft_url=https://github.com/yossigo/redisraft \
        --extra-vars redisraft_version=test-branch \
        --extra-vars "@instances.yml" ansible/site.yml

### Login to control node, initialize the cluster and run a quick benchmark

To login to the control node, you will have to have the SSH private key
available and run:

    ssh ubuntu@<control_node_public_addr>

Next, initialize the cluster:

    ./cluster.sh init

If you need to explicitly control shard placement, it is possible to manipulate
`cluster.sh` before running `init`.

You can now run `memtier_benchmark`. In order to get information about
endpoints, use:

    ./cluster.sh endpoints

You can then run a quick benchmark:

    memtier_benchmark --cluster -s <some endpoint addr> -p <some endpoint port>

### Changing configuration

To change a RedisRaft configuration parameter across all nodes, use:

    ./cluster.sh redisraft-config-set <param> <value>

To change a Redis configuration parameter across all nodes, use:

    ./cluster.sh redis-config-set <param> <value>

Tearing down
------------

To tear down the environment, simply run:

    terraform destroy -var-file=myfile.tfvars
