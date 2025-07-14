import {useRef} from 'react';
import {Button} from './button.tsx';
import {useImageUpload} from '../hooks/use-image-upload.ts';

interface ImageUploadButtonProps {
  onUpload: (markdown: string) => void;
}

export function ImageUploadButton({onUpload}: ImageUploadButtonProps) {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const {isUploading, uploadFile} = useImageUpload({onUpload});

  const handleFileChange = async (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }

    await uploadFile(file);
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
