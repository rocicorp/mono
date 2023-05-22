export default function Cube({rotation}: {rotation: number}) {
  const style = {
    transform: `rotate(${rotation}deg)`,
    transition: 'transform 0.2s ease-in',
  };

  return (
    <svg
      height="200px"
      width="200px"
      version="1.1"
      id="Layer_1"
      xmlns="http://www.w3.org/2000/svg"
      viewBox="-140 -150 800 800"
      fill="#000000"
      style={style}
    >
      <g id="SVGRepo_bgCarrier"></g>
      <g id="SVGRepo_tracerCarrier"></g>
      <g id="SVGRepo_iconCarrier">
        <polygon
          style={{fill: '#B4D8F1'}}
          points="480,112 256,0 32,112 32,400 256,512 480,400 "
        ></polygon>
        <polygon
          style={{fill: '#98C8ED'}}
          points="256,224 32,112 32,400 256,512 480,400 480,112 "
        ></polygon>
        <polygon
          style={{fill: '#7AB9E8'}}
          points="256,224 256,512 480,400 480,112 "
        ></polygon>
      </g>
    </svg>
  );
}
