#!/bin/bash

set -x

export OS_PROJECT_DOMAIN_NAME=default
export OS_USER_DOMAIN_NAME=default
export OS_PROJECT_NAME=admin
export OS_USERNAME=admin
export OS_PASSWORD=passw0rd
export OS_AUTH_URL=http://localhost:35357/v3
export OS_IDENTITY_API_VERSION=3
export OS_IMAGE_API_VERSION=2
export HOSTNAME=1space-keystone

_get_id()
{
	awk '/ id / { print $4 }'
}

_add_user()
{
	local tenant=$1
	local user=$2
	local password=$3
	local role=$4

	echo "Adding user $user"

	TENANT_ID=$(openstack project list | awk "/ $tenant / { print \$2 }")
	if [ "$TENANT_ID" == "" ]; then
		# create a new tenant
		TENANT_ID=$(openstack project create $tenant | _get_id)
	fi

	USER_ID=$(openstack user list | awk "/ $user / { print \$2 }")
	if [ "$USER_ID" == "" ]; then
		USER_ID=$(openstack user create $user --password=$password \
			--project $TENANT_ID | _get_id)
	fi

	if [ "$role" != "" ]; then
		ROLE_ID=$(openstack role list | awk "/ $role / { print \$2 }")
		if [ "$ROLE_ID" == "" ]; then
			# create a new role
			ROLE_ID=$(openstack role create $role | _get_id)
		fi

		openstack role add --user $USER_ID --project $TENANT_ID $ROLE_ID
	fi
}

/bin/bash /bootstrap.sh &

until echo > /dev/tcp/localhost/35357; do sleep 0.5; done >/dev/null 2>&1

_add_user service swift password admin
_add_user test admin admin admin
_add_user test tester testing admin

SERVICE=$(openstack service list | awk "/ swift / { print \$2 }")
if [ "$SERVICE" == "" ]; then
	echo "Adding service swift"
	openstack service create --name swift object-store
fi

ENDPOINT=$(openstack endpoint list | awk "/ swift / { print \$2 }")
if [ "$ENDPOINT" == "" ]; then
	echo "Adding an object store endpoint"
	openstack endpoint create object-store public 'http://swift-s3-sync:8080/v1/KEY_$(tenant_id)s'
fi

wait
