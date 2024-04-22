# Zeppliear

A test-bed app for Zero based on Repliear a Replicache based high-performance issue tracker in the style of [Linear](https://linear.app/).

Built with Zero and [Next.js](https://nextjs.org/),).

Running at [zeppliear.vercel.app](https://zeppliear.vercel.app/).

# To run locally

```
npm install
npx reflect dev --server-path backend/index.ts
NEXT_PUBLIC_SERVER='http://localhost:8080' npm run dev
```

# To deploy the reflect.net server

1. Login to the reflect cli as replicache@roci.dev
2. update `@rocicorp/reflect` version in package.json by prepending a `^`
3. `npx reflect publish --server-path backend/index.ts --app zeppliear`

## Credits

We started this project by forking [linear_clone](https://github.com/tuan3w/linearapp_clone). This enabled us to get the visual styling right much faster than we otherwise could have.
