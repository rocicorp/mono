import {
  useRef,
  useState,
  useEffect,
  useCallback,
  type ChangeEvent,
} from 'react';
import type {ReactNode} from 'react';
import classNames from 'classnames';
import {useLogin} from '../hooks/use-login.tsx';
import {Button} from './button.tsx';
import styles from './image-upload-area.module.css';

interface ImageUploadAreaProps {
  children: ReactNode;
  className?: string;
}

export function ImageUploadArea({
  children,
  className = '',
}: ImageUploadAreaProps) {
  const [isDragOver, setIsDragOver] = useState(false);
  const [textareaRect, setTextareaRect] = useState<DOMRect | null>(null);
  const [isUploading, setIsUploading] = useState(false);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const {loginState} = useLogin();

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
  ): Promise<{url: string; key: string}> => {
    const response = await fetch('/api/upload/presigned-url', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${loginState?.encoded}`,
      },
      body: JSON.stringify({contentType}),
    });

    if (!response.ok) {
      throw new Error(`Failed to get presigned URL: ${response.statusText}`);
    }

    return response.json();
  };

  // Textarea image insert logic (from use-textarea-image-insert.ts)
  const insertMarkdown = useCallback((markdown: string) => {
    if (wrapperRef.current) {
      const textarea = wrapperRef.current.querySelector('textarea');
      if (textarea) {
        const start = textarea.selectionStart;
        const end = textarea.selectionEnd;
        const text = textarea.value;
        const newText =
          text.substring(0, start) + markdown + text.substring(end);

        // Need to simulate user input here

        const nativeInputValueSetter = Object.getOwnPropertyDescriptor(
          window.HTMLTextAreaElement.prototype,
          'value',
        )?.set;

        if (nativeInputValueSetter) {
          nativeInputValueSetter.call(textarea, newText);
        } else {
          textarea.value = newText;
        }

        // Create a synthetic event that React will recognize as a real user input
        const event = new Event('input', {bubbles: true});

        // Make the event look like it came from user interaction
        Object.defineProperty(event, 'target', {
          writable: false,
          value: textarea,
        });

        Object.defineProperty(event, 'currentTarget', {
          writable: false,
          value: textarea,
        });

        // Dispatch the event, trigger React's onChange handler
        textarea.dispatchEvent(event);

        // Set cursor position after the inserted markdown
        setTimeout(() => {
          textarea.focus();
          textarea.setSelectionRange(
            start + markdown.length,
            start + markdown.length,
          );
        }, 0);
      }
    }
  }, []);

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
      const {url: presignedUrl, key} = await getPresignedUrl(file.type);

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
      insertMarkdown(markdown);
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
