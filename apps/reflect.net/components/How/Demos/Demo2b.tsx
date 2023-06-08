import {Prism as SyntaxHighlighter} from 'react-syntax-highlighter';
import {vscDarkPlus} from 'react-syntax-highlighter/dist/cjs/styles/prism';

export function Demo2b() {
  const codeString = `import {Reflect} from "@rocicorp/reflect/client";
import mutators from "./mutators";
const authToken = "$your-auth-token";
const roomID = "myFirstRoom";

const r = new Reflect({
  reflectKey,
  authToken,
  roomID,
  mutators, 
});

r.subscribe(tx => tx.get("degree"), val => {
  console.log(\`Key "degree" changed to: \${val}\`);
  document.querySelector("#degreeImage").rotation = val;
});

<Slider onChange={(val) => reflect.mutate.setDegree(val)} \\>`;

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
