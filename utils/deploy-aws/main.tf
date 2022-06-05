provider "aws" {
  region    = var.region
}

resource "aws_vpc" "vpc" {
  cidr_block = var.vpc-cidr

  tags = {
    Name = "vpc-${var.name}"
  }
}

resource "aws_internet_gateway" "gateway" {
  vpc_id = aws_vpc.vpc.id

  tags = {
    Name = "igw-${var.name}"
  }
}

resource "aws_route" "route" {
  route_table_id            = aws_vpc.vpc.main_route_table_id
  destination_cidr_block    = "0.0.0.0/0"
  gateway_id                = aws_internet_gateway.gateway.id
}

resource "aws_subnet" "main" {
  count                     = "${length(var.vpc-subnets)}"
  vpc_id                    = aws_vpc.vpc.id
  cidr_block                = "${var.vpc-subnets[count.index]}"
  map_public_ip_on_launch   = true
  availability_zone         = "${var.vpc-azs[count.index]}"
}

resource "aws_security_group" "default" {
  name          = "security_group_${var.name}"
  description   = "Security group for ${var.name}"
  vpc_id        = aws_vpc.vpc.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = -1
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.vpc.cidr_block]
  }
}

data "aws_ami" "ubuntu" {
  most_recent   = true

  filter {
    name    = "name"
    values  = ["ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*"]
  }

  owners = ["099720109477"]
}

resource "aws_instance" "node" {
  count             = var.replicas
  ami               = data.aws_ami.ubuntu.id
  instance_type     = var.instance_type

  subnet_id         = aws_subnet.main[count.index].id
  vpc_security_group_ids = [aws_security_group.default.id]

  key_name          = var.key_pair_name

  tags = {
    Name = "${var.name}-node-${count.index}"
  }

  root_block_device {
    volume_type = var.volume_type
    volume_size = var.volume_size
  }
}

resource "aws_instance" "control" {
  ami               = data.aws_ami.ubuntu.id
  instance_type     = var.control_instance_type

  subnet_id         = aws_subnet.main[0].id
  vpc_security_group_ids = [aws_security_group.default.id]

  key_name          = var.key_pair_name

  tags = {
    Name = "${var.name}-control"
  }

  root_block_device {
    volume_type = var.control_volume_type
    volume_size = var.control_volume_size
  }
}

####
#### Ansible part.
####

# Create an inventory
resource "local_file" "hosts_cfg" {
  content = templatefile("${path.module}/hosts.tpl",
    {
      nodes = aws_instance.node.*
      control = aws_instance.control.*
    }
  )
  filename = "ansible/inventory/hosts.cfg"
}

# Create instances.yml, used by Ansible to generate configuration.
resource "null_resource" "instances_yml" {
  provisioner "local-exec" {
    command = "./create_instances.sh --shards ${var.shards} --nodes ${var.replicas} --base-port ${var.base_port}"
  }
}

# Run Ansible to deploy cluster and control nodes
resource "null_resource" "ansible" {
  depends_on = [aws_instance.control, aws_instance.node]
  provisioner "local-exec" {
    command = "ansible-playbook --extra-vars @ansible/instances.yml ansible/site.yml"
  }
}
