import xxhash from 'xxhash-wasm';

const {create64, h32, h64} = await xxhash();

export {create64, h32, h64};
