#!/bin/bash

set -e

# Make sure all of the .pid files are removed -- services will not start
# otherwise
find /var/lib/ -name *.pid -delete
find /var/run/ -name *.pid -delete

# Include our own internal-client config.
# Ditto for proxy-server config, and a config that will run a no-auth, no-shunt
# proxy-server instance on port 8082.
cp -f /1space/containers/1space/{internal-client,proxy-server,proxy-server-noshunt}.conf /etc/swift/

# Copied from the docker swift container. Unfortunately, there is no way to
# plugin an additional invocation to start 1space-sync, so we had to do this.
/usr/sbin/service rsyslog start
/usr/sbin/service rsync start
/usr/sbin/service memcached start

# set up storage
mkdir -p /swift/nodes/1 /swift/nodes/2 /swift/nodes/3 /swift/nodes/4

for i in `seq 1 4`; do
    if [ ! -e "/srv/$i" ]; then
        ln -s /swift/nodes/$i /srv/$i
    fi
done
mkdir -p /srv/1/node/sdb1 /srv/2/node/sdb2 /srv/3/node/sdb3 /srv/4/node/sdb4 \
    /var/run/swift
/usr/bin/sudo /bin/chown -R swift:swift /swift/nodes /etc/swift /srv/1 /srv/2 \
    /srv/3 /srv/4 /var/run/swift
/usr/bin/sudo -u swift /swift/bin/remakerings

cd /1space; pip install -e .

/usr/bin/sudo -u swift /swift/bin/startmain

# Set up stuff for cloud-connector
# NOTE: s3cmd is installed via requirements-test.txt in the Dockerfile
set +e
s3cmd -c /1space/s3cfg info s3://$CONF_BUCKET
rc=$?
set -e

if [ $rc -ne 0 ]; then
    s3cmd -c /1space/s3cfg mb s3://$CONF_BUCKET
fi

s3cmd -c /1space/s3cfg put /1space/containers/1space/cloud-connector.conf \
    s3://$CONF_BUCKET
s3cmd -c /1space/s3cfg put /1space/containers/1space/1space.conf \
    s3://$CONF_BUCKET/sync-config.json
s3cmd -c /1space/s3cfg put /1space/containers/1space/s3-passwd.json \
    s3://$CONF_BUCKET/s3-passwd.json

/usr/local/bin/supervisord -n -c /etc/supervisord.conf
