import {nanoid} from 'nanoid';

function Page() {
  return '';
}

export function getServerSideProps() {
  const spaceID = nanoid(10);
  return {
    redirect: {
      destination: `/d/${spaceID}`,
      permanent: false,
    },
  };
}

export default Page;
