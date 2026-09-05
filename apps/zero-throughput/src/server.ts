import {fastify} from '../api/index.ts';

const port = Number(
  process.env.PORT ?? parsePortArg(process.argv.slice(2)) ?? 3000,
);
const host = process.env.HOST ?? '0.0.0.0';

function parsePortArg(args: readonly string[]): number | undefined {
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--port' && i + 1 < args.length) {
      return Number(args[i + 1]);
    }
    if (arg.startsWith('--port=')) {
      return Number(arg.slice('--port='.length));
    }
  }
  return undefined;
}

async function main() {
  try {
    const address = await fastify.listen({port, host});
    fastify.log.info(`zero-throughput API server listening on ${address}`);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
}

void main();
