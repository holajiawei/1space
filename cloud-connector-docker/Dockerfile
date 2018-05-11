# I am intended to be built from the top of the swift-s3-sync tree like this:
#    docker build [...] -f cloud-connector-docker/Dockerfile .

FROM python:2-alpine3.7 AS wheel
WORKDIR /opt/cloud-connector
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
    python-dev \
    zlib-dev && \
    echo "#include <sys/types.h>" >> /usr/include/sys/xattr.h && \
    git clone https://github.com/openstack/liberasurecode && \
    cd liberasurecode && \
    ./autogen.sh && \
    ./configure && \
    make install && \
    make DESTDIR=/liberasurecode-tree install
# Install non-cloud-builder Python dependencies
COPY cloud-connector-docker/*.txt requirements.txt ./
ARG SWIFT_REPO=openstack/swift
ARG SWIFT_TAG=2.17.0
RUN pip install --no-warn-script-location -I --root /python-root \
    -r ./requirements.txt -r ./cloud-connector-requirements.txt \
    "git+https://github.com/${SWIFT_REPO}@${SWIFT_TAG}#egg=swift"
COPY test/container/swift3-unicode-fix.patch /tmp/
RUN bash -c "cd /python-root/usr/local/lib/python2.7/site-packages \
    && patch -p1 < /tmp/swift3-unicode-fix.patch"
# Now install cloud-builder code itself
ARG SWIFT_S3_SYNC_DIR=.
COPY $SWIFT_S3_SYNC_DIR/setup.py ./
COPY $SWIFT_S3_SYNC_DIR/s3_sync ./s3_sync
RUN pip install --no-warn-script-location --no-deps -I --root /python-root .


#########################################


FROM python:2-alpine3.7
MAINTAINER Darrell Bishop <darrell@swiftstack.com>
WORKDIR /usr/src/cloud-connector
ARG CONF_BUCKET=cloud-connector-conf

# TODO: remove bash
RUN apk del libx11 libxrender libxft tk .python-rundeps && \
    apk --no-cache add \
    bash \
    ca-certificates \
    libxml2 \
    libxslt \
    sqlite-libs && \
    adduser -S -h /etc/swift swift swift

COPY --from=wheel /liberasurecode-tree /
COPY --from=wheel /python-root /

ENV CONF_BUCKET ${CONF_BUCKET}
ENV PYTHONPATH /usr/local/lib/python2.7/site-packages

# FIRE IN THE HOLE!!
CMD ["/usr/local/bin/cloud-connector", "-v"]
