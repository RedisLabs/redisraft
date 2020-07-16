#!/bin/bash
sudo apt-get -y -q update
sudo apt-get install -qqy \
    openjdk-8-jre \
    libjna-java \
    git \
    gnuplot \
    wget \
    vim \
    graphviz

export LEIN_ROOT=true
sudo wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
sudo mv lein /usr/bin/lein
sudo chmod 0755 /usr/bin/lein
lein self-install
