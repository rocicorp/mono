/* eslint-disable @typescript-eslint/no-explicit-any */
import type {FC} from 'react';
import {useRef} from 'react';
import Editor, {type Monaco} from '@monaco-editor/react';
import {Play, Key} from 'lucide-react';
import type {editor} from 'monaco-editor';
import zeroClientTypes from '../../bundled-types/zero-client.d.ts?raw';

interface QueryEditorProps {
  value: string;
  onChange: (value: string) => void;
  onExecute: () => void;
  onOpenCredentials: () => void;
  hasCredentials: boolean;
}

export const QueryEditor: FC<QueryEditorProps> = ({
  value,
  onChange,
  onExecute,
  onOpenCredentials,
  hasCredentials,
}) => {
  const monacoRef = useRef<any>(null);
  const editorRef = useRef<any>(null);

  const handleEditorDidMount = async (
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
        <div className="editor-buttons">
          <button
            onClick={onOpenCredentials}
            className={`credentials-button ${hasCredentials ? 'has-credentials' : ''}`}
            title="Set credentials for server authentication"
          >
            <Key size={16} />
            {hasCredentials ? 'Credentials Set' : 'Set Credentials'}
          </button>
          <button
            onClick={onExecute}
            className="execute-button"
            title="Execute Query (Ctrl+Enter)"
          >
            <Play size={16} />
            Execute
          </button>
        </div>
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
