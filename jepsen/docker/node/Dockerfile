FROM debian:stretch

# Install packages
RUN apt-get update && \
    apt-get -y install \
        openssh-server \
        pwgen \
        && \
mkdir -p /var/run/sshd && \
sed -i "s/UsePrivilegeSeparation.*/UsePrivilegeSeparation no/g" /etc/ssh/sshd_config && \
sed -i "s/PermitRootLogin without-password/PermitRootLogin yes/g" /etc/ssh/sshd_config

# Install Jepsen deps
RUN apt-get update && \
    apt-get -y install \
        apt-transport-https \
        software-properties-common \
        build-essential \
        bzip2 \
        curl \
        faketime \
        iproute \
        iptables \
        iputils-ping \
        libzip4 \
        logrotate \
        man \
        man-db \
        net-tools \
        ntpdate \
        psmisc \
        python \
        rsyslog \
        sudo \
        tar \
        unzip \
        vim \
        wget \
	tcpdump \
	git \
        cmake \
        automake \
        autoconf \
        libtool \
        && \
        apt-get remove -y --purge --auto-remove systemd

ADD entrypoint.sh /entrypoint.sh
RUN chmod 0755 /entrypoint.sh

EXPOSE 22
ENTRYPOINT ["/entrypoint.sh"]
