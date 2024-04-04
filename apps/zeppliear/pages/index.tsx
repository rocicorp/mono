import {transact} from '../backend/pg';
import {createDatabase, initSpace} from '../backend/data';
import {getReactSampleData} from '../backend/sample-issues';

function Page() {
  return '';
}

export async function getServerSideProps() {
  const spaceID = await transact(async executor => {
    await createDatabase(executor);
    return initSpace(executor, getReactSampleData);
  });
  return {
    redirect: {
      destination: `/d/${spaceID}`,
      permanent: false,
    },
  };
}

export default Page;
