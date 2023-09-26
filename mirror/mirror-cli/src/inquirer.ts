import confirmFn from '@inquirer/confirm';
import inputFn from '@inquirer/input';
import passwordFn from '@inquirer/password';
import selectFn from '@inquirer/select';

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

type PasswordConfig = AsyncPromptConfig & {
  mask?: boolean | string;
};

type Choice<Value> = {
  value: Value;
  name?: string;
  description?: string;
  disabled?: boolean | string;
  type?: never;
};

type SelectConfig<Value> = {
  message: string | Promise<string> | (() => Promise<string>);
  choices: readonly Choice<Value>[];
  pageSize?: number | undefined;
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

export function password(config: PasswordConfig): Promise<string> {
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore type error in jest?!?
  return passwordFn(config);
}

export function select<Value>(config: SelectConfig<Value>): Promise<Value> {
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore type error in jest?!?
  return selectFn(config);
}
