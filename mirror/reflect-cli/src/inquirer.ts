import confirmFn from '@inquirer/confirm';
import inputFn from '@inquirer/input';

// Copy types and wrap functions from the @inquirer library to consolidate
// the places where we ignore ts errors from jest.

type AsyncPromptConfig = {
  message: string | Promise<string> | (() => Promise<string>);
  validate?: (value: string) => boolean | string | Promise<string | boolean>;
};

type ConfirmConfig = AsyncPromptConfig & {
  message: string;
  default?: boolean;
};

type InputConfig = AsyncPromptConfig & {
  default?: string;
};

export function confirm(config: ConfirmConfig): Promise<boolean> {
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore type error in jest?!?
  return confirmFn(config);
}

export function input(config: InputConfig): Promise<string> {
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore type error in jest?!?
  return inputFn(config);
}
