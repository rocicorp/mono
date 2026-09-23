const selectStar = /\bselect\b[\s\S]*?\*(?=\s*(?:,|\bfrom\b))[\s\S]*?\bfrom\b/i;

// Commands that resolve to a `.CMD`/`.bat` shim on Windows rather than a real
// executable image. `CreateProcess` only ever appends `.exe`, so spawning one of
// these by bare name throws ENOENT on Windows even when the shim is on PATH.
// `shell: true` "fixes" it but concatenates arguments instead of escaping them
// (Node DEP0190), so the portable answer is to resolve the JS entrypoint and
// spawn that with `process.execPath`.
// Limited to what this repo actually runs: the two sites that prompted the rule
// (`npm` from the release scripts, `tsc` from the zero build), the package manager
// and Node's own launchers, and the CLIs it depends on. Names it has no
// relationship with are left out — an error-level rule should not be enumerating
// tools nobody here spawns.
const shimCommands = new Set([
  'npm',
  'npx',
  'pnpm',
  'tsc',
  'tsx',
  'vite',
  'vitest',
  'oxlint',
  'oxfmt',
  'turbo',
  'prisma',
  'drizzle-kit',
  'playwright',
  'sst',
]);

// `exec` and `execSync` are absent because they always go through a shell, which
// resolves `npm.cmd` itself.
//
// Reach: the command must be a string literal and the callee a plain or member
// identifier, and the callee's origin is not tracked.
const spawnFunctions = new Set([
  'spawn',
  'spawnSync',
  'execFile',
  'execFileSync',
]);

const whitespace = /\s+/;

const plugin = {
  meta: {
    name: 'zero',
    namespace: 'zero',
  },
  rules: {
    'no-select-star': {
      meta: {
        type: 'problem',
        docs: {
          description:
            'Disallow SELECT * in runtime SQL because schema migrations can change result shapes and break cached prepared plans.',
        },
        schema: [
          {
            type: 'object',
            properties: {
              include: {
                type: 'array',
                items: {type: 'string'},
              },
            },
            additionalProperties: false,
          },
        ],
        messages: {
          noSelectStar:
            'Avoid SELECT * in runtime SQL. Schema migrations can change result shapes and break cached prepared plans; list result columns explicitly.',
        },
      },
      create(context) {
        const options = context.options[0] ?? {};
        const filename = context.filename ?? context.getFilename?.() ?? '';
        if (!shouldCheck(filename, options.include ?? [])) {
          return {};
        }

        function check(node, text) {
          const stripped = stripCommentOnlyLines(text);
          if (hasSelectStar(stripped)) {
            context.report({node, messageId: 'noSelectStar'});
          }
        }

        return {
          Literal(node) {
            if (typeof node.value === 'string') {
              check(node, node.value);
            }
          },
          TemplateLiteral(node) {
            check(node, node.quasis.map(element => element.value.raw).join(''));
          },
        };
      },
    },

    'no-bare-shim-spawn': {
      meta: {
        type: 'problem',
        docs: {
          description:
            'Disallow spawning a command that is a .CMD shim on Windows by bare name; resolve its JS entrypoint and spawn it with process.execPath.',
        },
        schema: [],
        messages: {
          resolveEntrypoint:
            "Spawning '{{command}}' by bare name throws ENOENT on Windows: it is a .CMD shim and CreateProcess only appends .exe. Do not reach for `shell: true` — it concatenates arguments instead of escaping them (Node DEP0190). Resolve the JS entrypoint (createRequire(...).resolve) and spawn it with process.execPath.",
        },
      },
      create(context) {
        return {
          CallExpression(node) {
            const command = bareShimSpawnCommand(node);
            if (command !== undefined) {
              context.report({
                node,
                messageId: 'resolveEntrypoint',
                data: {command},
              });
            }
          },
        };
      },
    },
  },
};

function stripCommentOnlyLines(source) {
  return source
    .split('\n')
    .map(line => {
      const trimmed = line.trimStart();
      return trimmed.startsWith('//') || trimmed.startsWith('--') ? '' : line;
    })
    .join('\n');
}

function hasSelectStar(source) {
  return selectStar.test(source);
}

/**
 * Normalize a filesystem path to forward slashes, so the `/`-bearing comparisons
 * in `shouldCheck` mean the same thing wherever the linter runs.
 *
 * Comparing against a raw filename makes every one of them false where paths use
 * backslashes, and a rule that never matches a path never runs — which reads
 * exactly like a codebase with nothing to report.
 */
function toPosixPath(filename) {
  return filename.replaceAll('\\', '/');
}

function shouldCheck(filename, include) {
  const path = toPosixPath(filename);
  return (
    include.some(included => path.includes(toPosixPath(included))) &&
    !path.endsWith('.test.ts') &&
    !path.includes('/test/') &&
    !path.includes('/__snapshots__/') &&
    !path.endsWith('_generated.ts')
  );
}

function isStaticallyFalsy(node) {
  if (node?.type === 'Literal') {
    return !node.value;
  }
  if (node?.type === 'Identifier') {
    return node.name === 'undefined';
  }
  return node?.type === 'UnaryExpression' && node.operator === 'void';
}

/**
 * Returns the offending command name when `node` spawns a Windows shim command
 * by bare name without a shell, otherwise undefined.
 */
function bareShimSpawnCommand(node) {
  // Both spellings count: a bare `spawn(…)` and a `childProcess.spawn(…)` member
  // call are equally common, and `cp['spawn'](…)` is out of reach either way.
  const callee = node.callee;
  const calleeName =
    callee?.type === 'Identifier'
      ? callee.name
      : callee?.type === 'MemberExpression' && !callee.computed
        ? callee.property?.name
        : undefined;
  if (calleeName === undefined || !spawnFunctions.has(calleeName)) {
    return undefined;
  }
  const [commandArgument, ...rest] = node.arguments ?? [];
  if (
    commandArgument?.type !== 'Literal' ||
    typeof commandArgument.value !== 'string'
  ) {
    return undefined;
  }
  const command = commandArgument.value.trim().split(whitespace)[0];
  if (!shimCommands.has(command)) {
    return undefined;
  }
  // The rule name is the scope: a shim spawned by bare name WITHOUT a shell
  // cannot be resolved at all. `shell: true` does resolve it, at the different
  // cost DEP0190 describes, which is Node's warning to give and not this rule's.
  // So an explicit `shell` is a deliberate opt-out.
  if (rest.some(hasShellOption)) {
    return undefined;
  }
  return command;
}

/**
 * Whether an argument is an options object that opts into a shell.
 *
 * The options bag is located by shape, not by position: `args` is optional in
 * every one of these signatures, so `spawn('npm', {shell: true})` puts it at
 * index 1 — reading index 2 reported those calls despite the opt-out. A spread
 * is treated as opting in, because its contents are not knowable here and a
 * false negative is cheaper than an error on correct code.
 *
 * The VALUE decides, not the key's presence: `{shell: false}` is exactly the
 * configuration that fails, so reading only the key would exempt the call this
 * rule exists to report.
 */
function hasShellOption(argument) {
  if (argument?.type !== 'ObjectExpression') {
    return false;
  }
  return argument.properties.some(property => {
    if (property.type === 'SpreadElement') {
      return true;
    }
    if (
      property.type !== 'Property' ||
      (property.key?.name !== 'shell' && property.key?.value !== 'shell')
    ) {
      return false;
    }
    // A statically falsy value is not an opt-out — `{shell: false}` is the very
    // configuration that fails, and `{shell: undefined}` / `{shell: void 0}` are
    // that same configuration spelled differently. Anything else — `true`, a
    // variable, a call — is not knowably falsy, so it counts as deliberate.
    return !isStaticallyFalsy(property.value);
  });
}

export default plugin;
export {
  bareShimSpawnCommand,
  hasSelectStar,
  shouldCheck,
  stripCommentOnlyLines,
  toPosixPath,
};
