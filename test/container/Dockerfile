FROM bouncestorage/swift-aio:latest

RUN apt-get update && \
    apt-get install --no-install-recommends -y python-support openjdk-7-jdk \
        wget curl && \
    apt-get clean

RUN pip install -e git://github.com/swiftstack/botocore.git@1.4.32.6#egg=botocore && \
    pip install boto3==1.3.1 && \
    pip install -e git://github.com/swiftstack/container-crawler.git@0.0.12#egg=container-crawler && \
    pip install nose flake8==3.5.0 && \
    pip uninstall -y hacking

RUN mkdir -p /s3proxy/data
RUN cd /s3proxy && wget https://github.com/andrewgaul/s3proxy/releases/download/s3proxy-1.5.3/s3proxy

RUN mkdir /swift-s3-sync && mkdir /var/lib/swift-s3-sync
EXPOSE 10080
RUN locale-gen en_US.UTF-8
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'
CMD ["/bin/bash", "/swift-s3-sync/test/container/launch.sh"]
