if [ -z "$VERCEL_PRODUCTION_BUILD" ]
then
  npm run publish-worker-staging
else
  npm run publish-worker-prod
fi