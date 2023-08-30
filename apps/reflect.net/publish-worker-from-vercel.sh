#!/bin/bash
if [ "$VERCEL_PRODUCTION_BUILD" ]
then
  echo "Production build pushing prod worker"
  npm run publish-worker-prod
elif [ "$VERCEL_SANDBOX_BUILD" ]
then
  echo "Sandbox build pushing sandbox worker"
  npm run publish-worker-sandbox
else
  echo "Preview build pushing preview worker"
  npm run publish-worker-preview
fi