import type {FC, SVGProps} from 'react';

// eslint-disable-next-line @typescript-eslint/naming-convention
export const MarkIcon: FC<SVGProps<SVGSVGElement>> = props => (
  <svg
    width="18"
    height="18"
    viewBox="0 0 24 24"
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
    {...props}
  >
    <title>Show Zero Inspector</title>
    <path
      fillRule="evenodd"
      clipRule="evenodd"
      d="M0.407235 15.1075C-0.661857 11.1041 0.374032 6.65546 3.51478 3.51471C8.20106 -1.17157 15.799 -1.17157 20.4854 3.51471C20.8969 3.92627 21.2723 4.36029 21.6115 4.81284L17.6063 8.81802H12.7576L16.7275 4.84814C13.3994 2.64322 8.87135 3.00687 5.9391 5.93909C4.24366 7.63455 3.40687 9.86362 3.42896 12.0857L0.407235 15.1075ZM18.0609 18.0609C15.1287 20.9931 10.6006 21.3568 7.27247 19.1519L11.2423 15.182H6.39356L2.38844 19.1872C2.72767 19.6397 3.10316 20.0737 3.51466 20.4853C8.20094 25.1716 15.799 25.1716 20.4852 20.4853C23.626 17.3445 24.6619 12.8959 23.5927 8.89255L20.5709 11.9143C20.593 14.1364 19.7564 16.3654 18.0609 18.0609Z"
      fill="currentColor"
    />
  </svg>
);
