import React, {useState, useEffect} from 'react';

const colors: {rgb: string; name: string}[] = [
  {rgb: '39, 252, 174', name: 'Green'},
  {rgb: '255, 92, 0', name: 'Orange'},
  {rgb: '252, 33, 138', name: 'Reflect pink'},
  {rgb: '252, 33, 113', name: 'Pink 2'},
  {rgb: '252, 33, 179', name: 'Pink 3'},
  {rgb: '39, 252, 220', name: 'Green 2'},
];

// Create data URI so we can dynamically change svg color (for closed issue icon)
const createSvgDataUri = (color: string): string => {
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 14 14" fill="none">
      <g clip-path="url(#clip0_112_115)">
        <g clip-path="url(#clip1_112_115)">
          <path d="M9.545 3.545L9.9 3.9L9.546 3.546L5.5 7.591L4.455 6.545C4.32959 6.41959 4.1807 6.32011 4.01684 6.25223C3.85298 6.18436 3.67736 6.14943 3.5 6.14943C3.32264 6.14943 3.14702 6.18436 2.98316 6.25223C2.8193 6.32011 2.67041 6.41959 2.545 6.545C2.41959 6.67041 2.32011 6.8193 2.25223 6.98316C2.18436 7.14702 2.14943 7.32264 2.14943 7.5C2.14943 7.67736 2.18436 7.85298 2.25223 8.01684C2.32011 8.1807 2.41959 8.32959 2.545 8.455L4.545 10.455C4.67037 10.5805 4.81925 10.68 4.98311 10.7479C5.14698 10.8159 5.32262 10.8508 5.5 10.8508C5.67738 10.8508 5.85302 10.8159 6.01689 10.7479C6.18075 10.68 6.32963 10.5805 6.455 10.455L11.455 5.455C11.5805 5.32963 11.68 5.18075 11.7479 5.01689C11.8159 4.85302 11.8508 4.67738 11.8508 4.5C11.8508 4.32262 11.8159 4.14698 11.7479 3.98311C11.68 3.81925 11.5805 3.67037 11.455 3.545L11.1 3.9L11.454 3.546C11.3286 3.42052 11.1798 3.32098 11.0159 3.25306C10.852 3.18514 10.6764 3.15019 10.499 3.15019C10.3216 3.15019 10.146 3.18514 9.98211 3.25306C9.81825 3.32098 9.66937 3.42052 9.544 3.546L9.545 3.545ZM0.5 7C0.5 5.27609 1.18482 3.62279 2.40381 2.40381C3.62279 1.18482 5.27609 0.5 7 0.5C8.72391 0.5 10.3772 1.18482 11.5962 2.40381C12.8152 3.62279 13.5 5.27609 13.5 7C13.5 8.72391 12.8152 10.3772 11.5962 11.5962C10.3772 12.8152 8.72391 13.5 7 13.5C5.27609 13.5 3.62279 12.8152 2.40381 11.5962C1.18482 10.3772 0.5 8.72391 0.5 7Z" fill="${color}" stroke="${color}" />
        </g>
      </g>
    </svg>`;
  return `data:image/svg+xml;utf8,${encodeURIComponent(svg)}`;
};

const ColorTester: React.FC = () => {
  const [currentColorIndex, setCurrentColorIndex] = useState<number>(0);

  useEffect(() => {
    const currentColor = colors[currentColorIndex].rgb;
    const currentName = colors[currentColorIndex].name;

    // Update the primary color and dimmed color
    document.documentElement.style.setProperty(
      '--color-primary-cta',
      `rgba(${currentColor}, 1)`,
    );
    document.documentElement.style.setProperty(
      '--color-primary-cta-dimmed',
      `rgba(${currentColor}, 0.2)`,
    );

    // Conditionally update text color for Green and Green 2
    if (currentName === 'Green' || currentName === 'Green 2') {
      document.documentElement.style.setProperty(
        '--color-primary-cta-text',
        'rgba(0, 0, 0, 1)',
      );
    } else {
      document.documentElement.style.setProperty(
        '--color-primary-cta-text',
        'rgba(255, 255, 255, 1)',
      );
    }

    // Dynamically generate and apply the data URI for the SVG
    const svgDataUri = createSvgDataUri(`rgb(${currentColor})`);

    // Debugging the generated URI to verify correctness
    console.log('Generated SVG Data URI:', svgDataUri);

    // Add a <style> tag to the document head for the dynamic rule
    const dynamicStyleTagId = 'dynamic-svg-style';
    let dynamicStyleTag = document.getElementById(
      dynamicStyleTagId,
    ) as HTMLStyleElement;

    if (!dynamicStyleTag) {
      dynamicStyleTag = document.createElement('style');
      dynamicStyleTag.id = dynamicStyleTagId;
      document.head.appendChild(dynamicStyleTag);
    }

    // Assign the rule content to the <style> tag
    dynamicStyleTag.textContent = `.primary-content .issue-closed { background-image: url("${svgDataUri}") !important; }`;
  }, [currentColorIndex]);

  const handleColorChange = (): void => {
    const nextIndex = (currentColorIndex + 1) % colors.length;
    setCurrentColorIndex(nextIndex);
  };

  const currentColor = colors[currentColorIndex];

  return (
    <div
      style={{
        position: 'fixed',
        top: '10px',
        right: '10px',
        background: 'black',
        border: '1px solid lightgray',
        padding: '10px',
        borderRadius: '5px',
        zIndex: 1000,
        cursor: 'pointer',
        color: 'white',
      }}
      onClick={handleColorChange}
    >
      <p style={{margin: 0, fontSize: '12px'}}>Click to Change Color</p>
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          marginTop: '5px',
        }}
      >
        <div
          style={{
            width: '24px',
            height: '24px',
            backgroundColor: `rgb(${currentColor.rgb})`,
            borderRadius: '50%',
            border: '1px solid white',
            marginRight: '10px',
          }}
        />
        <span style={{fontSize: '14px'}}>{currentColor.name}</span>
      </div>
    </div>
  );
};

export default ColorTester;
