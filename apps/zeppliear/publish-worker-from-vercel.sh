#!/bin/bash
if [ "$VERCEL_PRODUCTION_BUILD" ]
then
  echo "Production build pushing prod worker"
  npm run publish-worker-prod
fi