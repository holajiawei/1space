# I am intended to be built from the top of the swift-s3-sync tree like this:
#    docker build [...] -f containers/1space/Dockerfile .

FROM bouncestorage/swift-aio:latest

RUN apt-get update && \
    apt-get install --no-install-recommends -y \
        wget curl patch git locales net-tools gcc \
        python-dev && \
    apt-get clean

COPY requirements.txt requirements-test.txt /tmp/
RUN pip install -U -r /tmp/requirements.txt -r /tmp/requirements-test.txt && \
    pip uninstall -y hacking && pip install keystonemiddleware

RUN mkdir /1space && mkdir /var/lib/1space
RUN locale-gen en_US.UTF-8
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'

# This patch allows tempauth to use Keystone-provided identities ("groups") for
# authz decisions.
COPY containers/1space/tempauth-keystone-groups.patch /tmp/
RUN bash -c "cd /usr/local/src \
    && patch -p0 < /tmp/tempauth-keystone-groups.patch \
    && rm -f /tmp/tempauth-keystone-groups.patch"

# TODO: remove swift3 once cloud connector is migrated to s3api
RUN git clone --branch 1.12 --single-branch --depth 1 https://github.com/openstack/swift3.git /usr/local/src/swift3 && \
    cd /usr/local/src/swift3 && python setup.py develop

# Have rsyslog listen on UDP port 514
COPY containers/1space/rsyslog.conf /etc/rsyslog.conf
RUN touch /var/log/syslog && chown syslog:adm /var/log/syslog

ENV CONF_BUCKET=cloud-connector-conf

CMD ["/bin/bash", "/1space/containers/1space/launch.sh"]
