import {useRef, useState, useEffect} from 'react';
import type {ReactNode} from 'react';
import {useImageUpload} from '../hooks/use-image-upload.ts';
import {Button} from './button.tsx';

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
  const dragCounterRef = useRef(0);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const {isUploading, uploadFiles} = useImageUpload({onUpload});

  const handleFileSelect = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []);
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

    // Update rect on mount and when dragging
    updateRect();
    if (isDragOver) {
      updateRect();
    }
  }, [isDragOver]);

  const handleDragEnter = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounterRef.current++;

    if (e.dataTransfer.items && e.dataTransfer.items.length > 0) {
      setIsDragOver(true);
    }
  };

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragCounterRef.current--;

    if (dragCounterRef.current === 0) {
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
    dragCounterRef.current = 0;

    const files = Array.from(e.dataTransfer.files).filter(file =>
      file.type.startsWith('image/'),
    );

    if (files.length > 0) {
      await uploadFiles(files);
    }
  };

  const handlePaste = async (e: React.ClipboardEvent) => {
    const items = Array.from(e.clipboardData.items);
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

  const dropZoneClasses = `
    ${className}
    ${isDragOver ? 'drag-over' : ''}
    ${isUploading ? 'uploading' : ''}
  `.trim();

  return (
    <div
      ref={wrapperRef}
      className={dropZoneClasses}
      onDragEnter={handleDragEnter}
      onDragLeave={handleDragLeave}
      onDragOver={handleDragOver}
      onDrop={handleDrop}
      onPaste={handlePaste}
      style={{
        position: 'relative',
      }}
    >
      {children}
      {isDragOver && textareaRect && (
        <div
          style={{
            position: 'absolute',
            top: textareaRect.top,
            left: textareaRect.left,
            width: textareaRect.width,
            height: textareaRect.height,
            backgroundColor: 'rgba(59, 130, 246, 0.1)',
            border: '2px dashed #3b82f6',
            borderRadius: '4px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '14px',
            color: '#3b82f6',
            fontWeight: 'medium',
            pointerEvents: 'none',
            zIndex: 10,
          }}
        >
          Drop images here to upload
        </div>
      )}
      {isUploading && textareaRect && (
        <div
          style={{
            position: 'absolute',
            top: textareaRect.top + textareaRect.height / 2,
            left: textareaRect.left + textareaRect.width / 2,
            transform: 'translate(-50%, -50%)',
            backgroundColor: 'rgba(0, 0, 0, 0.8)',
            color: 'white',
            padding: '8px 16px',
            borderRadius: '4px',
            fontSize: '14px',
            zIndex: 20,
            pointerEvents: 'none',
          }}
        >
          Uploading image...
        </div>
      )}
      {/* Image upload button positioned inside textarea */}
      {textareaRect && (
        <Button
          className="add-image-button secondary-button icon-button"
          eventName="Upload image"
          onAction={handleButtonClick}
          disabled={isUploading}
          style={{
            position: 'absolute',
            top: textareaRect.top + 16,
            left: textareaRect.left + 16,
            fontSize: '12px',
            padding: '4px 8px',
            zIndex: 5,
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
        style={{display: 'none'}}
      />
    </div>
  );
}
