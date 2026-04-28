const selectStar = /\bselect\s+\*\s+from\b/i;

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
        const filename = normalizePath(
          context.filename ?? context.getFilename?.() ?? '',
        );
        if (!shouldCheck(filename, options.include ?? [])) {
          return {};
        }

        function check(node, text) {
          const stripped = stripCommentOnlyLines(text);
          if (selectStar.test(stripped)) {
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

function normalizePath(source) {
  return source.replaceAll('\\', '/');
}

function shouldCheck(filename, include) {
  return (
    include.some(path => filename.includes(path)) &&
    !filename.endsWith('.test.ts') &&
    !filename.includes('/test/') &&
    !filename.includes('/__snapshots__/') &&
    !filename.endsWith('_generated.ts')
  );
}

export default plugin;
