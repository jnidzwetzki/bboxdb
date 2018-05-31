###
# Build
###

# Release:
# docker build --build-arg version=tags/v0.0.1 -t jnidzwetzki/bboxdb:0.0.1 - < misc/docker/Dockerfile

# Git head
# docker build --build-arg version=master -t jnidzwetzki/bboxdb:latest --no-cache - < misc/docker/Dockerfile

###
# Run
###

# docker run --rm -e 'ZK_HOSTS=zk1, zk2, zk3' --name bboxdb1 jnidzwetzki/bboxdb:latest

##########################################################################################

FROM alpine/git as clone
ARG version
RUN git clone https://github.com/jnidzwetzki/bboxdb.git /bboxdb
WORKDIR /bboxdb
RUN git checkout ${version}

FROM maven:3.5-jdk-8-alpine as build
WORKDIR /bboxdb
COPY --from=clone /bboxdb /bboxdb
RUN mvn install -DskipTests
RUN echo "storageDirectories: " >> /bboxdb/conf/bboxdb.yaml \
     && echo " - /bboxdb/storage" >> /bboxdb/conf/bboxdb.yaml

FROM openjdk:8-jre-alpine
WORKDIR /bboxdb
COPY --from=build /bboxdb /bboxdb
RUN apk update && apk add bash
ENV BBOXDB_HOME=/bboxdb
ENV BBOXDB_FOREGROUND=true

# BBoxDB database port
EXPOSE 50505/tcp

# Performance counter (prometheus)
EXPOSE 10085/tcp

RUN mkdir -p /bboxdb/storage/data
VOLUME /bboxdb/storage

CMD ["/bboxdb/misc/docker/docker-entrypoint.sh"]
