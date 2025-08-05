import classNames from 'classnames';
import type {ReactNode} from 'react';
import {useCallback, useRef, useState, type ChangeEvent} from 'react';
import {useLogin} from '../hooks/use-login.tsx';
import {Button} from './button.tsx';
import styles from './image-upload-area.module.css';

export type TextAreaSelection = {start: number; end: number};

export type TextAreaPatch = {
  /** Where the original selection was when the action happened */
  selection: TextAreaSelection;
  /** The text being inserted/replacement text */
  text: string;
  /** Suggested caret/selection to set after your state update */
  nextSelection: TextAreaSelection;
  /** Pure function to produce the next value */
  apply: (prev: string) => string;
};

type ImageUploadAreaProps = {
  children: ReactNode;
  className?: string;
  textAreaRef: React.RefObject<HTMLTextAreaElement>;
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
  const [isDragOver, setIsDragOver] = useState(false);
  const [isUploading, setIsUploading] = useState(false);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
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

      const patch: TextAreaPatch = {
        selection: sel,
        text: markdown,
        nextSelection: {
          start: start + markdown.length,
          end: start + markdown.length,
        },
        apply: (prev: string) =>
          prev.slice(0, start) + markdown + prev.slice(end),
      };

      onInsert(patch);

      requestAnimationFrame(() => focusAndSetSelection(patch.nextSelection));
    },
    [getSelection, onInsert, textAreaRef, focusAndSetSelection],
  );

  const uploadFile = async (file: File): Promise<void> => {
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
      console.error('Error uploading image:', error);
      alert('An error occurred while uploading the image. Please try again.');
    } finally {
      setIsUploading(false);
    }
  };

  const uploadFiles = async (files: File[]): Promise<void> => {
    for (const file of files) {
      await uploadFile(file);
    }
  };

  const handleFileSelect = async (e: ChangeEvent<HTMLInputElement>) => {
    const files = [...(e.target.files ?? [])];
    if (files.length > 0) {
      await uploadFiles(files);
    }
    // Reset the input so the same file can be selected again
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  const handleButtonClick = () => {
    fileInputRef.current?.click();
  };

  const handleDragEnter = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();

    if (e.dataTransfer.items && e.dataTransfer.items.length > 0) {
      setIsDragOver(true);
    }
  };

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();

    // Only clear drag state if we're leaving the wrapper entirely
    // Check if the relatedTarget (where we're going) is outside our wrapper
    const wrapper = e.currentTarget as HTMLElement;
    const relatedTarget = e.relatedTarget as HTMLElement;

    if (!relatedTarget || !wrapper.contains(relatedTarget)) {
      setIsDragOver(false);
    }
  };

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  };

  const handleDrop = async (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragOver(false);

    const files = [...e.dataTransfer.files].filter(file =>
      file.type.startsWith('image/'),
    );

    if (files.length > 0) {
      await uploadFiles(files);
    }
  };

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
    'drag-over': isDragOver,
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
      ref={wrapperRef}
      className={classNames(dropZoneClasses, styles.wrapper)}
      onDragEnter={handleDragEnter}
      onDragLeave={handleDragLeave}
      onDragOver={handleDragOver}
      onDrop={handleDrop}
      onPaste={handlePaste}
    >
      {children}
      {isDragOver && textAreaRect && (
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
          onAction={handleButtonClick}
          disabled={isUploading}
          style={{
            top: textAreaRect.top + 16,
            left: textAreaRect.left + 16,
          }}
        >
          Add image
        </Button>
      )}
      {/* Hidden file input */}
      <input
        ref={fileInputRef}
        type="file"
        accept="image/jpeg,image/png,image/webp,image/gif"
        multiple
        onChange={handleFileSelect}
        className={styles.hiddenInput}
      />
    </div>
  );
}
