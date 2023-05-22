[[ -z "$VERCEL_PRODUCTION_BUILD" ]] && exit 0;

npm run publish-worker-prod
