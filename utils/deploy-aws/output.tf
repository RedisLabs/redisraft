output "cluster_node_private_addr" {
   description = "Private addresses of cluster nodes"
   value = aws_instance.node.*.private_ip
}

output "cluster_node_public_addr" {
   description = "Public addresses of cluster nodes"
   value = aws_instance.node.*.public_ip
}

output "control_node_public_addr" {
    description = "Public address of the control node"
    value = aws_instance.control.public_ip
}
