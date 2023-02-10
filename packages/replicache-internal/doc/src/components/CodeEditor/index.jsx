import React from 'react';
import {Sandpack} from '@codesandbox/sandpack-react';
import extractFiles from './file';

export default function CodeEditor(props) {
  let {children, template = 'vanilla', externalResources = []} = props;
  // convert the children to an array
  let codeSnippets = React.Children.toArray(children);
  // using the array.reduce method to reduce the children to an object containing
  // filename as key then other properties like the code, if the file is hidden as
  // properties
  const files = extractFiles(codeSnippets, template);
  return (
    <Sandpack
      template={template}
      files={files}
      customSetup={{
        dependencies: {replicache: 'latest'},
      }}
      options={{
        showLineNumbers: true,
        showInlineErrors: false,
        showTabs: true,
        showNavigator: true,
        externalResources,
      }}
    />
  );
}
