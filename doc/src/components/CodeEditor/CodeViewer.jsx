import React from 'react';
import {
  SandpackProvider,
  SandpackCodeViewer,
  SandpackLayout,
  nightOwlTheme,
} from '@codesandbox/sandpack-react';
import extractFiles from './file.js';

export default function CodeEditor(props) {
  let {children, template = 'react', externalResources = []} = props;

  // convert the children to an array
  let codeSnippets = React.Children.toArray(children);

  // using the array.reduce method to reduce the children to an object containing
  // filename as key then other properties like the code, if the file is hidden as
  // properties
  const files = extractFiles(codeSnippets, template);
  return (
    <SandpackProvider
      key="sandpack-provider"
      template={template}
      customSetup={{
        files,
      }}
    >
      <SandpackLayout theme={nightOwlTheme}>
        <SandpackCodeViewer showTabs showLineNumbers={true} showInlineErrors />
      </SandpackLayout>
    </SandpackProvider>
  );
}
