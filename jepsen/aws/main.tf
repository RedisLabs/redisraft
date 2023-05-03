provider "aws" {
  profile   = "default"
  region    = var.aws_region
}

resource "aws_vpc" "vpc" {
  cidr_block = "10.0.0.0/16"

  tags = {
    Name = "vpc-${var.project}"
  }
}

resource "aws_internet_gateway" "gateway" {
  vpc_id = aws_vpc.vpc.id

  tags = {
    Name = "igw-${var.project}"
  }
}

resource "aws_route" "route" {
  route_table_id            = aws_vpc.vpc.main_route_table_id
  destination_cidr_block    = "0.0.0.0/0"
  gateway_id                = aws_internet_gateway.gateway.id
}

resource "aws_subnet" "main" {
  count                     = 1
  vpc_id                    = aws_vpc.vpc.id
  cidr_block                = "10.0.1.0/24"
  map_public_ip_on_launch   = true
  availability_zone         = var.aws_az
} 
  
resource "aws_security_group" "default" {
  name          = "${var.project}_security_group"
  description   = "Security group for ${var.project}"
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
    from_port   = 6379
    to_port     = 6379
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.vpc.cidr_block]
  }
}

data "aws_ami" "debian" {
  most_recent   = true

  filter {
    name    = "name"
    values  = ["debian-10-amd64-*"]
  }

  owners = ["379101102735"]
}

resource "tls_private_key" "sshkey" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_instance" "node" {
  count             = var.nodes_count

  ami               = data.aws_ami.debian.id
  instance_type     = var.aws_instance_type

  subnet_id         = aws_subnet.main[0].id
  vpc_security_group_ids = [aws_security_group.default.id]

  key_name          = var.aws_key_pair_name

  user_data         = <<-EOT
    ${file("scripts/install_node.sh")}
    sudo mkdir -p /root/.ssh
    echo '${tls_private_key.sshkey.public_key_openssh}' | sudo tee -a /root/.ssh/authorized_keys > /dev/null
    echo '${tls_private_key.sshkey.private_key_pem}' | sudo tee /root/.ssh/id_rsa > /dev/null
    ssh-keyscan -t rsa github.com | sudo tee -a /root/.ssh/known_hosts
    sudo chmod 0600 /root/.ssh/id_rsa
  EOT

  root_block_device {
    volume_type = var.aws_volume_type
    volume_size = var.node_volume_size
  }
    
  tags = {
    Name = "${var.project}-node-${count.index}"
  }
}



resource "aws_instance" "control" {
  ami               = data.aws_ami.debian.id
  instance_type     = var.aws_instance_type

  subnet_id         = aws_subnet.main[0].id
  vpc_security_group_ids = [aws_security_group.default.id]

  key_name          = var.aws_key_pair_name

  tags = {
    Name = "${var.project}-control"
  }

  root_block_device {
    volume_type = var.aws_volume_type
    volume_size = var.control_volume_size
  }

  user_data         = <<-EOT
    ${file("scripts/install_control.sh")}

    mkdir -p ~${var.control_user}/.ssh
    echo '${tls_private_key.sshkey.private_key_pem}' > ~${var.control_user}/.ssh/id_rsa
    chmod 0600 ~${var.control_user}/.ssh/id_rsa
    chown -R ${var.control_user}:${var.control_user} ~${var.control_user}/.ssh
  
    %{ for ip in aws_instance.node.*.private_ip }
    echo ${ip} >> ~${var.control_user}/nodes.txt
    %{ endfor }
    chown ${var.control_user}:${var.control_user} ~${var.control_user}/nodes.txt
  EOT
}

