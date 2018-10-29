FROM python:2-alpine3.7
MAINTAINER Thiago da Silva <thiago@swiftstack.com>
WORKDIR /opt/nas-connector
# NOTE: the sys/types.h thing is a hack to allow python xattr package to build
# on Alpine Linux.
RUN apk --no-cache add \
    autoconf \
    automake \
    build-base \
    ca-certificates \
    git \
    libffi-dev \
    libressl-dev \
    libtool \
    libxml2-dev \
    libxslt-dev \
    linux-headers \
    supervisor \
    python-dev \
    memcached \
    bash \
    ca-certificates \
    libxml2 \
    libxslt \
    sqlite-libs \
    zlib-dev && \
    echo "#include <sys/types.h>" >> /usr/include/sys/xattr.h && \
    git clone https://github.com/openstack/liberasurecode && \
    cd liberasurecode && \
    ./autogen.sh && \
    ./configure && \
    make install

ARG SWIFT_REPO=openstack/swift
ARG SWIFT_TAG=2.19.0
RUN git clone --single-branch --depth 1 https://github.com/${SWIFT_REPO} --branch ${SWIFT_TAG} && \
    cd swift && \
    pip install -r requirements.txt && \
    python setup.py install && \
    cd - 

RUN addgroup -S swift && adduser -S -h /etc/swift -G swift swift

# Now install nas connector code itself
ARG NAS_CONNECTOR_DIR=.
COPY $NAS_CONNECTOR_DIR/nas_connector_setup.py ./setup.py
COPY $NAS_CONNECTOR_DIR/nas_connector_requirements.txt ./requirements.txt
COPY $NAS_CONNECTOR_DIR/nas_connector/ ./nas_connector
COPY $NAS_CONNECTOR_DIR/containers/nas_connector/bin/nasconnector-gen-builders ./bin/
RUN pip install -r requirements.txt && \
    python setup.py install

RUN mkdir /var/run/swift && chown swift:swift /var/run/swift

# Replace openstack swift conf files with local gluster-swift ones
COPY containers/nas_connector/swift/* /etc/swift/
COPY containers/nas_connector/supervisord/supervisord.conf /etc/supervisord.d/swift.ini

VOLUME /srv/nasconnector

# build rings
RUN nasconnector-gen-builders nasconnector

ENV PYTHONPATH /usr/local/lib/python2.7/site-packages

# FIRE IN THE HOLE!!
CMD ["/usr/bin/supervisord", "-n", "-c", "/etc/supervisord.d/swift.ini"]
