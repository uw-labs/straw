#!/bin/bash
if [ "$S3_TEST_BUCKET" != "" ] ; then 
	echo "clearing s3 bucket"
	aws s3 rb --force s3://$S3_TEST_BUCKET/
	aws s3 mb s3://$S3_TEST_BUCKET/
else
	echo "S3_TEST_BUCKET" not set, skipping.
fi

if [ "$GCS_TEST_BUCKET" != "" ] ; then 
	echo "clearing gcs bucket"
	gsutil -m rm -r gs://$GCS_TEST_BUCKET/*
else
	echo "GCS_TEST_BUCKET" not set, skipping.
fi


