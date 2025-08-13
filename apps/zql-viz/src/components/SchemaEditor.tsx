import type { FC } from 'react';
import Editor from '@monaco-editor/react';
import { FileCode } from 'lucide-react';

interface SchemaEditorProps {
  value: string;
  onChange: (value: string) => void;
}

export const SchemaEditor: FC<SchemaEditorProps> = ({
  value,
  onChange,
}) => {
  return (
    <div className="schema-editor">
      <div className="editor-header">
        <h3>
          <FileCode size={16} />
          Schema Definition
        </h3>
      </div>
      <Editor
        height="100%"
        defaultLanguage="typescript"
        value={value}
        onChange={(val) => onChange(val || '')}
        theme="vs-dark"
        options={{
          minimap: { enabled: false },
          fontSize: 14,
          lineNumbers: 'on',
          automaticLayout: true,
          scrollBeyondLastLine: false,
          wordWrap: 'on',
        }}
      />
    </div>
  );
};