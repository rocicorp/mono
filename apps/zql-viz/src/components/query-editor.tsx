/* eslint-disable @typescript-eslint/no-explicit-any */
import Editor, {type Monaco} from '@monaco-editor/react';
import {Play} from 'lucide-react';
import type {editor} from 'monaco-editor';
import type {FC} from 'react';
import {useRef} from 'react';
import zeroClientTypes from '../../bundled-types/zero-client.d.ts?raw';

interface QueryEditorProps {
  value: string;
  onChange: (value: string) => void;
  onExecute: () => void;
}

export const QueryEditor: FC<QueryEditorProps> = ({
  value,
  onChange,
  onExecute,
}) => {
  const monacoRef = useRef<any>(null);
  const editorRef = useRef<any>(null);

  const handleEditorDidMount = (
    editor: editor.IStandaloneCodeEditor,
    monaco: Monaco,
  ) => {
    monacoRef.current = monaco;
    editorRef.current = editor;

    monaco.languages.typescript.typescriptDefaults.setCompilerOptions({
      target: monaco.languages.typescript.ScriptTarget.Latest,
      allowNonTsExtensions: true,
      moduleResolution: monaco.languages.typescript.ModuleResolutionKind.NodeJs,
      module: monaco.languages.typescript.ModuleKind.ESNext,
      noEmit: true,
      typeRoots: ['node_modules/@types'],
      // moduleDetection: monaco.languages.typescript.ModuleDetection.Force,
    });

    monaco.languages.typescript.typescriptDefaults.setDiagnosticsOptions({
      noSemanticValidation: false,
      noSyntaxValidation: false,
    });

    monaco.languages.typescript.typescriptDefaults.addExtraLib(
      zeroClientTypes,
      'node_modules/@types/@rocicorp/zero/index.d.ts',
    );

    monaco.languages.typescript.typescriptDefaults.addExtraLib(
      `import * as z from '@rocicorp/zero';
      declare global {
        const zero: typeof z;
        function run(query: any): any;
      }`,
      'global.d.ts',
    );
  };

  return (
    <div className="query-editor">
      <div className="editor-header">
        <h3>Query Editor</h3>
        <button
          onClick={onExecute}
          className="execute-button"
          title="Execute Query (Ctrl+Enter)"
        >
          <Play size={16} />
        </button>
      </div>
      <Editor
        height="100%"
        defaultLanguage="typescript"
        value={value}
        onChange={val => onChange(val || '')}
        onMount={handleEditorDidMount}
        theme="vs-dark"
        options={{
          minimap: {enabled: false},
          fontSize: 14,
          lineNumbers: 'on',
          automaticLayout: true,
          scrollBeyondLastLine: false,
          wordWrap: 'on',
          suggest: {
            showMethods: true,
            showFunctions: true,
            showConstructors: true,
            showFields: true,
            showVariables: true,
            showClasses: true,
            showStructs: true,
            showInterfaces: true,
            showModules: true,
            showProperties: true,
          },
        }}
      />
    </div>
  );
};
