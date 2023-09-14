import styled from 'styled-components';

/* eslint-disable @typescript-eslint/no-explicit-any */
const checkSaturation = (props: any) => {
  if (props.scroll < 26) {
    return 100 - props.scroll * 4 + '%';
  }
  return '0%';
};

// eslint-disable-next-line @typescript-eslint/naming-convention
export const NavLogo = styled.img`
  transition: filter 0.1s ease-in-out;
  width: auto;
  height: 44px;
  -o-object-fit: contain;
  object-fit: contain;
  -o-object-position: 0% 50%;
  object-position: 0% 50%;
  filter: saturate(${checkSaturation});
`;
