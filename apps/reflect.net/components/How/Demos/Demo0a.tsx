import {Prism as SyntaxHighlighter} from 'react-syntax-highlighter';
import {vscDarkPlus} from 'react-syntax-highlighter/dist/cjs/styles/prism';

export function Demo0a() {
  const codeString = `import {Reflect} from "@rocicorp/reflect/client";

const reflect = new Reflect({
  reflectAPIKey,
  roomID: "myFirstRoom",
});

reflect.onConnect(({roomID}) => {
  console.log(\`Connected to room \${roomID}\`);
});`;
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
