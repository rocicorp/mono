import xxhash from './xxhash/index.js';

const {create64, h64} = await xxhash();

export {create64, h64};
