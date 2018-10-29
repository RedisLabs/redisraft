FROM debian:jessie
RUN apt-get update && apt-get -y install build-essential autoconf automake libtool cmake libbsd-dev curl
ADD build.sh .
RUN chmod a+rx build.sh
VOLUME /dist
VOLUME /src
CMD ./build.sh
