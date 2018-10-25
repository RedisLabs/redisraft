FROM debian:jessie
RUN apt-get update && apt-get -y install build-essential autoconf automake libtool cmake libbsd-dev git
ADD build.sh .
RUN chmod a+rx build.sh
VOLUME /dist
CMD ./build.sh
