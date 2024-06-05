type Navigator = {
  onLine: boolean;
  userAgent: string;
  // add more as needed
};

const localNavigator: Navigator | undefined =
  typeof navigator !== 'undefined' ? navigator : undefined;

export {localNavigator as navigator};
