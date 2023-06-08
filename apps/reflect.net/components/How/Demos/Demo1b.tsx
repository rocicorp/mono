import {Prism as SyntaxHighlighter} from 'react-syntax-highlighter';
import {vscDarkPlus} from 'react-syntax-highlighter/dist/cjs/styles/prism';

export function Demo1b() {
  const codeString = `import {Reflect} from "@rocicorp/reflect/client";
import mutators from "./mutators";
const roomID = "myFirstRoom";

const reflect = new Reflect({
  reflectKey,
  roomID,
  mutators,
});

reflect.subscribe(tx => tx.get("count"), val => {
  console.log(\`Key "count" changed to: \${val}\`);
});

button.onclick = () => reflect.mutate.increment({key: "count", delta: 1});`;
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
