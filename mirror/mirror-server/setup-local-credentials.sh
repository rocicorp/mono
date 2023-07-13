#!/bin/sh

CREDS_FILE='.local.application_default_credentials.json'

if [ -a $CREDS_FILE ]
then
  echo "Using $CREDS_FILE"
  exit
fi

gcloud auth application-default login --impersonate-service-account functions@reflect-mirror-staging.iam.gserviceaccount.com
mv $HOME/.config/gcloud/application_default_credentials.json $CREDS_FILE
echo "Moved credentials to $CREDS_FILE"
