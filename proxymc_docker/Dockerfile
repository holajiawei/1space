FROM ubuntu:16.04
MAINTAINER Darrell Bishop <darrell@swiftstack.com>

# NOTE: Dockerfile best practices say to avoid "apt-get dist-upgrade" or
# equiv.
RUN apt-get update && apt-get install --no-install-recommends -y \
    build-essential \
    curl \
    git \
    liberasurecode-dev \
    libffi-dev \
    memcached \
    ntp \
__S3PROXY_BEGIN__
    openjdk-8-jdk-headless \
__S3PROXY_END__
    python-dev \
    python-pip \
    python-setuptools \
    rsyslog \
    supervisor \
    wget \
 && rm -rf /var/lib/apt/lists/* \
 && pip install -U pip

COPY files/swift/requirements.txt /tmp/swift-requirements.txt
RUN pip install -r /tmp/swift-requirements.txt

COPY files/swift-s3-sync/requirements.txt /tmp/swift-s3-sync-requirements.txt
RUN pip install -r /tmp/swift-s3-sync-requirements.txt

RUN pip install requests python-swiftclient

# OPTIONAL: install S3Proxy
__S3PROXY_BEGIN__
RUN mkdir -p /s3proxy/data \
    && cd /s3proxy \
    && wget https://github.com/andrewgaul/s3proxy/releases/download/s3proxy-1.5.3/s3proxy
RUN pip install s3cmd
__S3PROXY_END__

# Supervisord config file
COPY .supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Add rsyslog configuration files
COPY files/0-swift.conf /etc/rsyslog.d/0-swift.conf
COPY files/50-default.conf /etc/rsyslog.d/50-default.conf
COPY files/rsyslog.conf /etc/rsyslog.conf
RUN chown -R syslog:adm /var/log

# The build_docker_image.py command will replace this "template variable"
ENV CONF_BUCKET __CONFBUCKET__
__S3PROXY_BEGIN__
ENV CONF_ENDPOINT http://localhost:10080
__S3PROXY_END__
ENV PYTHONPATH /usr/local/lib/python2.7/site-packages

# Set up the service user (swift)
RUN adduser --system --home /etc/swift --group swift

# OPTIONAL: put some S3Proxy configuration files and data into the right
#           bucket
__S3PROXY_BEGIN__
COPY files/s3proxy.properties /s3proxy/
COPY files/s3cfg /root/.s3cfg
COPY files/proxymc.conf files/swift-s3-sync.conf /tmp/
RUN /bin/bash -c '/usr/bin/java -DLOG_LEVEL=debug -jar /s3proxy/s3proxy \
    --properties /s3proxy/s3proxy.properties & \
    sleep 4; \
    s3cmd mb s3://__CONFBUCKET__; \
    s3cmd put /tmp/proxymc.conf s3://__CONFBUCKET__; \
    s3cmd put /tmp/swift-s3-sync.conf s3://__CONFBUCKET__/swift-s3-sync/test/container/swift-s3-sync.conf; \
    rm /tmp/{proxymc,swift-s3-sync}.conf; \
    kill %1'
__S3PROXY_END__

# Copy in our code
COPY files/swift/tree/ /
COPY files/swift-s3-sync/tree /

# FIRE IN THE HOLE!!
CMD ["/usr/bin/supervisord"]
