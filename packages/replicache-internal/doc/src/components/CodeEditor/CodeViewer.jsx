import React from 'react';
import {
  SandpackProvider,
  SandpackCodeEditor,
  SandpackLayout,
  SandpackPreview,
  useSandpack,
} from '@codesandbox/sandpack-react';

import extractFiles from './file.js';
import {useEffect} from 'react';

export default function CodeEditor(props) {
  let {children, template = 'vanilla'} = props;

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
      files={files}
      customSetup={{
        dependencies: {replicache: 'latest'},
        configurations: {},
      }}
    >
      <SandpackLayout>
        <SandpackCodeEditor showTabs showLineNumbers={true} showInlineErrors />
        <SandpackPreview></SandpackPreview>
        <ListenerIframeMessage />
      </SandpackLayout>
    </SandpackProvider>
  );
}

const ListenerIframeMessage = () => {
  const {sandpack} = useSandpack();

  const sender = () => {
    Object.values(sandpack.clients).forEach(client => {
      console.log('client', client);
      client.iframe.contentWindow.postMessage('Hello World', '*');
    });
  };

  useEffect(() => {
    sender();
  });

  return null;
};
