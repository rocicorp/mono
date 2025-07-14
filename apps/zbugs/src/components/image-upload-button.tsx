import {useRef, useState} from 'react';
import {Button} from './button.tsx';
import {getPresignedUrl} from '../server/upload.ts';

interface ImageUploadButtonProps {
  onUpload: (markdown: string) => void;
}

export function ImageUploadButton({onUpload}: ImageUploadButtonProps) {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [isUploading, setIsUploading] = useState(false);

  const handleFileChange = async (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }

    //
    // Validation
    //
    const validTypes = ['image/jpeg', 'image/png', 'image/webp'];
    if (!validTypes.includes(file.type)) {
      alert('Invalid file type. Please select a JPG, PNG, or WEBP image.');
      return;
    }

    if (file.size > 10 * 1024 * 1024) {
      alert('File is too large. Maximum size is 10MB.');
      return;
    }

    //
    // Upload
    //
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

  const handleClick = () => {
    fileInputRef.current?.click();
  };

  return (
    <>
      <input
        type="file"
        ref={fileInputRef}
        onChange={handleFileChange}
        style={{display: 'none'}}
        accept="image/png, image/jpeg, image/webp"
      />
      <Button onAction={handleClick} disabled={isUploading}>
        {isUploading ? 'Uploading...' : 'Add Image'}
      </Button>
    </>
  );
}
