import {Prism as SyntaxHighlighter} from 'react-syntax-highlighter';
import {vscDarkPlus} from 'react-syntax-highlighter/dist/cjs/styles/prism';

const Demo1a = () => {
  const codeString = `export default {
  async increment(tx, args: {key: string, delta: number}) {
    const {key, delta} = args;
    const prev = await tx.get(key, 0);
    const next = prev + delta;
    console.log(\`Running mutation \${tx.clientID}@\${tx.mutationID} \` +
                \`on \${tx.environment}: \${prev} → \${next}\`);
    await tx.put(key, next);
  },
}`;
  const codeBlock = {
    background: 'transparent',
    paddingLeft: 0,
    paddingRight: 0,
  };

  return (
    <SyntaxHighlighter
      language="typescript"
      showLineNumbers
      customStyle={codeBlock}
      style={vscDarkPlus}
    >
      {codeString}
    </SyntaxHighlighter>
  );
};

export default Demo1a;
