[nodes]
%{ for i in nodes ~}
${i.public_ip} private_ip=${i.private_ip}
%{ endfor ~}

[control]
%{ for i in control ~}
${i.public_ip}
%{ endfor ~}

