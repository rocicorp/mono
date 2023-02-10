import { nanoid } from "nanoid";

function Page() {
  return "";
}

export function getServerSideProps() {
  return {
    redirect: {
      destination: `/d/${nanoid(6)}`,
      permanent: false,
    },
  };
}

export default Page;
