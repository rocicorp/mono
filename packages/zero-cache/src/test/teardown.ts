import {testDBs} from './db.js';

module.exports = async function () {
  await testDBs.end();
};
