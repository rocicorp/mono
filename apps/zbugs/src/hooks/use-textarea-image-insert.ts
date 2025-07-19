import {useCallback, type RefObject} from 'react';

export function useTextareaImageInsert(
  textareaRef: RefObject<HTMLTextAreaElement>,
  setValue: (newValue: string) => void,
) {
  const insertMarkdown = useCallback(
    (markdown: string) => {
      const textarea = textareaRef.current;
      if (textarea) {
        const start = textarea.selectionStart;
        const end = textarea.selectionEnd;
        const text = textarea.value;
        const newText =
          text.substring(0, start) + markdown + text.substring(end);
        setValue(newText);

        // Set cursor position after the inserted markdown
        setTimeout(() => {
          textarea.focus();
          textarea.setSelectionRange(
            start + markdown.length,
            start + markdown.length,
          );
        }, 0);
      }
    },
    [textareaRef, setValue],
  );

  return insertMarkdown;
}
