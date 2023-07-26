# reflect-scaffold-example

This project will set you up with a very basic application that you can immediately publish and see working utilizing reflect.

## To Run

```
npm install
npm run build
npx @rocicorp/reflect publish ./src/worker/index.ts --name=<name of your project>
# set your env file for VITE_WORKER_URL to the published worker url (i.e. name-of-project.reflect-server.net)
npm run dev
```
