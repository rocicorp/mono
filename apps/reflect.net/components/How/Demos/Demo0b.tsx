import {Prism as SyntaxHighlighter} from 'react-syntax-highlighter';
import {vscDarkPlus} from 'react-syntax-highlighter/dist/cjs/styles/prism';

export function Demo0b() {
  const codeString = `import {createReflectServer} from "@rocicorp/reflect/client";

export createReflectServer({
  authHandler: (authToken) => {
    return fetch("https://myserver.com/api/auth-reflect", {
      method: "POST",
      body: JSON.stringify({authToken}),
    });
  },
})`;
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
