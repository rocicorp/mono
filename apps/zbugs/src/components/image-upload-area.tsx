import {useRef, useState, useEffect, type ChangeEvent} from 'react';
import type {ReactNode} from 'react';
import classNames from 'classnames';
import {useImageUpload} from '../hooks/use-image-upload.ts';
import {Button} from './button.tsx';
import styles from './image-upload-area.module.css';

interface ImageUploadAreaProps {
  onUpload: (markdown: string) => void;
  children: ReactNode;
  className?: string;
}

export function ImageUploadArea({
  onUpload,
  children,
  className = '',
}: ImageUploadAreaProps) {
  const [isDragOver, setIsDragOver] = useState(false);
  const [textareaRect, setTextareaRect] = useState<DOMRect | null>(null);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const {isUploading, uploadFiles} = useImageUpload({onUpload});

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

  // Update textarea rect when drag starts or component mounts
  useEffect(() => {
    const updateRect = () => {
      if (wrapperRef.current) {
        // Find textarea within the wrapper
        const textarea = wrapperRef.current.querySelector('textarea');
        if (textarea) {
          // Get the textarea's position relative to the wrapper using offsetTop/offsetLeft
          setTextareaRect({
            top: textarea.offsetTop,
            left: textarea.offsetLeft,
            width: textarea.offsetWidth,
            height: textarea.offsetHeight,
          } as DOMRect);
        }
      }
    };

    updateRect();
  }, [isDragOver]);

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
      {isDragOver && textareaRect && (
        <div
          className={styles.dragOverlay}
          style={{
            top: textareaRect.top,
            left: textareaRect.left,
            width: textareaRect.width,
            height: textareaRect.height,
          }}
        >
          Drop images here to upload
        </div>
      )}
      {isUploading && textareaRect && (
        <div
          className={styles.uploadingOverlay}
          style={{
            top: textareaRect.top + textareaRect.height / 2,
            left: textareaRect.left + textareaRect.width / 2,
          }}
        >
          Uploading image...
        </div>
      )}
      {/* Image upload button positioned inside textarea */}
      {textareaRect && (
        <Button
          className={classNames(
            'add-image-button secondary-button icon-button',
            styles.uploadButton,
          )}
          eventName="Upload image"
          onAction={handleButtonClick}
          disabled={isUploading}
          style={{
            top: textareaRect.top + 16,
            left: textareaRect.left + 16,
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
