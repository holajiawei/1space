# Builder

FROM maven:3.6-jdk-8-alpine as builder

RUN wget https://github.com/jclouds/jclouds/archive/master.zip && \
    unzip master.zip && rm master.zip && \
    wget https://github.com/gaul/s3proxy/archive/master.zip && \
    unzip master.zip && rm master.zip

RUN cd /jclouds-master && mvn install -DskipTests=true -Dmaven.javadoc.skip=true -pl :jclouds-allblobstore
RUN cd /s3proxy-master && \
    sed -i \
    's/<jclouds\.version>.*<\/jclouds\.version>/<jclouds\.version>2.2.0-SNAPSHOT<\/jclouds\.version>/' \
    pom.xml && mvn package -DskipTests=true -Dmaven.javadoc.skip=true

# Main container

FROM openjdk:8-jre-alpine

WORKDIR /s3proxy/

COPY --from=builder \
    /s3proxy-master/target/s3proxy \
    /s3proxy/

COPY containers/s3proxy/s3proxy.conf /s3proxy/
EXPOSE 10080

ENTRYPOINT ["/usr/bin/java", "-jar", "/s3proxy/s3proxy"]
CMD ["--properties", "/s3proxy/s3proxy.conf"]
