#!/bin/bash
if [ -z "$VERCEL_PRODUCTION_BUILD" ]
then
  echo "Not a production build pushing staging worker"
  npm run publish-worker-staging
else
  echo "Production build pushing prod worker"
  npm run publish-worker-prod
fi