import {Prism as SyntaxHighlighter} from 'react-syntax-highlighter';
import {vscDarkPlus} from 'react-syntax-highlighter/dist/cjs/styles/prism';

export function Demo2a() {
  const codeString = `export default {
    async setDegree(tx, degree: number) {
      console.log(\`Running mutation \${tx.clientID}@\${tx.mutationID} \` +
                  \`on \${tx.environment}: \${degree}\`);
      await tx.put("degree", degree);
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
}
