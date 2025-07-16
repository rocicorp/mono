import {useState} from 'react';
import {useLogin} from './use-login.tsx';

interface UseImageUploadOptions {
  onUpload: (markdown: string) => void;
}

export function useImageUpload({onUpload}: UseImageUploadOptions) {
  const [isUploading, setIsUploading] = useState(false);
  const {loginState} = useLogin();

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

  const getPresignedUrl = async (contentType: string): Promise<{url: string; key: string}> => {
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

      // 4. Call onUpload callback
      onUpload(markdown);
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

  return {
    isUploading,
    uploadFiles,
  };
}
