import checkboxFn, {Separator} from '@inquirer/checkbox';
import confirmFn from '@inquirer/confirm';
import inputFn from '@inquirer/input';
import passwordFn from '@inquirer/password';

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

export type Choice<Value> = {
  name?: string;
  value: Value;
  disabled?: boolean | string;
  checked?: boolean;
  type?: never;
};

export type Item<Value> = Separator | Choice<Value>;

type CheckboxConfig<Value> = {
  message: string | Promise<string> | (() => Promise<string>);
  prefix?: string | undefined;
  pageSize?: number | undefined;
  instructions?: string | boolean | undefined;
  choices: readonly Item<Value>[];
  loop?: boolean | undefined;
  required?: boolean | undefined;
  validate?:
    | ((
        items: readonly Item<Value>[],
      ) => string | boolean | Promise<string | boolean>)
    | undefined;
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
export function checkbox<T>(config: CheckboxConfig<T>): Promise<T[]> {
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore type error in jest?!?
  return checkboxFn(config);
}
