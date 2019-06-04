FROM ubuntu:16.04
ENV LEIN_ROOT true

#
# Jepsen dependencies
#
RUN apt-get -y -q update && \
    apt-get install -qqy \
        openjdk-8-jre \
        libjna-java \
        git \
        gnuplot \
        wget \
	vim

RUN wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && \
    mv lein /usr/bin && \
    chmod +x /usr/bin/lein && \
    lein self-install

ADD jepsen /jepsen
RUN cd /jepsen && lein install

ADD ./init.sh /init.sh
RUN chmod +x /init.sh

CMD /init.sh
