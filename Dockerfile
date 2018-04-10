# Build with: docker build --build-arg version=0.0.1 -t jnidzwetzki/crypto-bot:0.0.1 - < Dockerfile

FROM alpine/git as clone
ARG version
WORKDIR /bboxdb
RUN git clone https://github.com/jnidzwetzki/crypto-bot.git /bboxdb
RUN git checkout tags/v${version}

FROM maven:3.5-jdk-8-alpine as build
WORKDIR /bboxdb
COPY --from=clone /bboxdb /bboxdb
RUN mvn install

FROM openjdk:8-jre-alpine
WORKDIR /bboxdb
COPY --from=build /bboxdb /bboxdb
ENTRYPOINT ["sh", "-c"]
ENV BBOXDB_HOME=/bboxdb
ENV BBOXDB_FOREGROUND=true
CMD ["/bboxdb/bin"]
