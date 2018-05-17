# I am intended to be built from the top of the swift-s3-sync tree like this:
#    docker build [...] -f test/container/Dockerfile .

FROM bouncestorage/swift-aio:latest

RUN apt-get update && \
    apt-get install --no-install-recommends -y \
        wget curl patch git build-essential python-dev locales \
        openjdk-8-jre-headless net-tools && \
    apt-get clean

COPY requirements.txt requirements-test.txt /tmp/
RUN pip install -U -r /tmp/requirements.txt -r /tmp/requirements-test.txt && \
    pip uninstall -y hacking

RUN mkdir -p /s3proxy/data && \
    cd /s3proxy && wget https://github.com/andrewgaul/s3proxy/releases/download/s3proxy-1.5.3/s3proxy

RUN mkdir /swift-s3-sync && mkdir /var/lib/swift-s3-sync
EXPOSE 10080
RUN locale-gen en_US.UTF-8
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'

# These patches improve Unicode handling in tempauth, swiftclient, and swift3
COPY test/container/tempauth.patch /tmp/
RUN bash -c "cd /usr/local/src \
    && patch -p0 < /tmp/tempauth.patch \
    && rm -f /tmp/tempauth.patch"
COPY test/container/swiftclient.patch /tmp/
RUN bash -c "cd /usr/local/src \
    && patch -p0 < /tmp/swiftclient.patch \
    && rm -f /tmp/swiftclient.patch"
COPY test/container/swift3-unicode-fix.patch /tmp/
RUN bash -c "cd /usr/local/src \
    && patch -p0 < /tmp/swift3-unicode-fix.patch \
    && rm -f /tmp/swift3-unicode-fix.patch"

ENV CONF_BUCKET=cloud-connector-conf

CMD ["/bin/bash", "/swift-s3-sync/test/container/launch.sh"]
