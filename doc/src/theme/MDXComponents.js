import MDXComponents from '@theme-original/MDXComponents';
import CodeEditor from '@site/src/components/CodeEditor';

export default {
  // Re-use the default mapping
  ...MDXComponents,
  // Map the "CodeEditor" tag to our <CodeEditor /> component!
  CodeEditor: CodeEditor,
};
