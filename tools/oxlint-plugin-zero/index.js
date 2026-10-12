const selectStar = /\bselect\b[\s\S]*?\*(?=\s*(?:,|\bfrom\b))[\s\S]*?\bfrom\b/i;

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

export default plugin;
export {hasSelectStar, shouldCheck, stripCommentOnlyLines, toPosixPath};
