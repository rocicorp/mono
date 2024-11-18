import {config} from '../../packages/shared/src/tool/vitest-config.js';

// No need for browser tests yet
config.test.browser.enabled = false;

export {config as default};
