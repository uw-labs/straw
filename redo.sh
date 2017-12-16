#!/bin/bash
if [ "$S3_TEST_BUCKET" == "" ] ; then 
	echo "S3_TEST_BUCKET" not set
	exit -1
fi

aws --profile dev s3 rb --force s3://$S3_TEST_BUCKET/
aws --profile dev s3 mb s3://$S3_TEST_BUCKET/
