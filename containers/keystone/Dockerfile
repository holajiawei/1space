# I am intended to be built from the top of the swift-s3-sync tree like this:
#    docker build [...] -f containers/keystone/Dockerfile .

FROM stephenhsu/keystone:9.1.0

COPY ./containers/keystone/setup_keystone.sh /
CMD /setup_keystone.sh
