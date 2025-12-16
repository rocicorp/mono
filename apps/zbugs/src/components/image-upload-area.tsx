import classNames from 'classnames';
import type {ReactNode} from 'react';
import {useCallback, useState} from 'react';
import {useDropzone, type FileRejection} from 'react-dropzone';
import {useLogin} from '../hooks/use-login.tsx';
import {Button} from './button.tsx';
import styles from './image-upload-area.module.css';

export type TextAreaSelection = {start: number; end: number};

export type TextAreaPatch = {
  /** Suggested caret/selection to set after your state update */
  nextSelection: TextAreaSelection;
  /** Pure function to produce the next value */
  apply: (prev: string) => string;
};

type ImageUploadAreaProps = {
  children: ReactNode;
  className?: string;
  textAreaRef: React.RefObject<HTMLTextAreaElement | null>;
  onInsert: (patch: TextAreaPatch) => void;
};

// Image upload logic (from use-image-upload.ts)
const validateFile = (file: File): string | null => {
  const validTypes = ['image/jpeg', 'image/png', 'image/webp', 'image/gif'];
  if (!validTypes.includes(file.type)) {
    return 'Invalid file type. Please select a JPG, PNG, WEBP, or GIF image.';
  }

  if (file.size > 10 * 1024 * 1024) {
    return 'File is too large. Maximum size is 10MB.';
  }

  return null;
};

const getPresignedUrl = async (
  contentType: string,
  jwt: string | undefined,
): Promise<{url: string; key: string}> => {
  if (!jwt) {
    throw new Error('No JWT provided');
  }

  const response = await fetch('/api/upload/presigned-url', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${jwt}`,
    },
    body: JSON.stringify({contentType}),
  });

  if (!response.ok) {
    throw new Error(`Failed to get presigned URL: ${response.statusText}`);
  }

  return response.json();
};

export function ImageUploadArea({
  children,
  className = '',
  textAreaRef,
  onInsert,
}: ImageUploadAreaProps) {
  const [isUploading, setIsUploading] = useState(false);
  const {loginState} = useLogin();

  const getSelection = useCallback((): TextAreaSelection | null => {
    const ta = textAreaRef.current;
    if (!ta) return null;
    const start = ta.selectionStart ?? 0;
    const end = ta.selectionEnd ?? start;
    return {start, end};
  }, [textAreaRef]);

  const focusAndSetSelection = useCallback(
    (sel: TextAreaSelection) => {
      const ta = textAreaRef.current;
      if (!ta) return;

      ta.focus();
      ta.setSelectionRange(sel.start, sel.end);
    },
    [textAreaRef],
  );

  const insert = useCallback(
    (markdown: string) => {
      const sel = getSelection();
      const ta = textAreaRef.current;
      if (!sel || !ta) return;

      const {start, end} = sel;
      const prevValue = ta.value ?? '';
      const insertionText =
        prevValue.length === 0 ? `${markdown}\n` : `\n${markdown}\n`;

      const patch: TextAreaPatch = {
        nextSelection: {
          start: start + insertionText.length,
          end: start + insertionText.length,
        },
        apply: (prev: string) => {
          const before = prev.slice(0, start);
          const after = prev.slice(end);
          return before + insertionText + after;
        },
      };

      onInsert(patch);

      requestAnimationFrame(() => focusAndSetSelection(patch.nextSelection));
    },
    [getSelection, onInsert, textAreaRef, focusAndSetSelection],
  );

  const uploadFile = useCallback(
    async (file: File): Promise<void> => {
      const validationError = validateFile(file);
      if (validationError) {
        alert(validationError);
        return;
      }

      if (!loginState) {
        alert('You must be logged in to upload images.');
        return;
      }

      setIsUploading(true);
      try {
        // 1. Get presigned URL
        const {url: presignedUrl, key} = await getPresignedUrl(
          file.type,
          loginState?.encoded,
        );

        // 2. Upload to S3
        await fetch(presignedUrl, {
          method: 'PUT',
          body: file,
          headers: {
            'Content-Type': file.type,
          },
        });

        // 3. Get public URL and create markdown
        const imageUrl = `https://zbugs-image-uploads.s3.amazonaws.com/${key}`;
        const markdown = `![${file.name}](${imageUrl})`;

        // 4. Insert markdown into textarea
        insert(markdown);
      } catch (error) {
        // oxlint-disable-next-line no-console -- Error logging in demo app
        console.error('Error uploading image:', error);
        alert('An error occurred while uploading the image. Please try again.');
      } finally {
        setIsUploading(false);
      }
    },
    [insert, loginState],
  );

  const uploadFiles = useCallback(
    async (files: File[]): Promise<void> => {
      for (const file of files) {
        await uploadFile(file);
      }
    },
    [uploadFile],
  );

  const onDrop = useCallback(
    async (acceptedFiles: File[], fileRejections: FileRejection[]) => {
      if (fileRejections && fileRejections.length > 0) {
        // Build an error message from the first rejection
        const first = fileRejections[0];
        const msg = first.errors?.map(e => e.message).join('\n');
        alert(msg || 'One or more files were rejected.');
      }
      if (acceptedFiles && acceptedFiles.length > 0) {
        await uploadFiles(acceptedFiles);
      }
    },
    [uploadFiles],
  );

  const {getRootProps, getInputProps, isDragActive, open} = useDropzone({
    accept: {
      'image/jpeg': [],
      'image/png': [],
      'image/webp': [],
      'image/gif': [],
    },
    multiple: true,
    maxSize: 10 * 1024 * 1024,
    noClick: true,
    noKeyboard: true,
    onDrop,
  });

  const handlePaste = async (e: React.ClipboardEvent) => {
    const items = [...e.clipboardData.items];
    const imageItems = items.filter(item => item.type.startsWith('image/'));

    if (imageItems.length > 0) {
      e.preventDefault();

      const files: File[] = [];
      for (const item of imageItems) {
        const file = item.getAsFile();
        if (file) {
          files.push(file);
        }
      }

      if (files.length > 0) {
        await uploadFiles(files);
      }
    }
  };

  const dropZoneClasses = classNames(className, {
    'drag-over': isDragActive,
    'uploading': isUploading,
  });

  const textAreaRect = {
    top: textAreaRef.current?.offsetTop ?? 0,
    left: textAreaRef.current?.offsetLeft ?? 0,
    width: textAreaRef.current?.offsetWidth ?? 0,
    height: textAreaRef.current?.offsetHeight ?? 0,
  };

  return (
    <div
      {...getRootProps({onPaste: handlePaste})}
      className={classNames(dropZoneClasses, styles.wrapper)}
    >
      {children}
      {isDragActive && textAreaRect && (
        <div
          className={styles.dragOverlay}
          style={{
            top: textAreaRect.top,
            left: textAreaRect.left,
            width: textAreaRect.width,
            height: textAreaRect.height,
          }}
        >
          Drop images here to upload
        </div>
      )}
      {isUploading && textAreaRect && (
        <div
          className={styles.uploadingOverlay}
          style={{
            top: textAreaRect.top + textAreaRect.height / 2,
            left: textAreaRect.left + textAreaRect.width / 2,
          }}
        >
          Uploading image...
        </div>
      )}
      {/* Image upload button positioned inside textarea */}
      {textAreaRect && (
        <Button
          className={classNames(
            'add-image-button secondary-button icon-button',
            styles.uploadButton,
          )}
          eventName="Upload image"
          onAction={open}
          disabled={isUploading}
          style={{
            top: textAreaRect.top + 16,
            left: textAreaRect.left + 16,
          }}
          tabIndex={-1}
        >
          Add image
        </Button>
      )}
      {/* Hidden file input (managed by react-dropzone) */}
      <input {...getInputProps()} className={styles.hiddenInput} />
    </div>
  );
}
