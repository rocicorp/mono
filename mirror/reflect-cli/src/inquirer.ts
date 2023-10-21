import confirmFn from '@inquirer/confirm';
import inputFn from '@inquirer/input';
import checkboxFn, {Separator} from '@inquirer/checkbox';

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

type Choice<Value> = {
  name?: string;
  value: Value;
  disabled?: boolean | string;
  checked?: boolean;
  type?: never;
};

type CheckboxConfig<T> = {
  message: string | Promise<string> | (() => Promise<string>);
  prefix?: string | undefined;
  pageSize?: number | undefined;
  instructions?: string | boolean | undefined;
  choices: readonly (Separator | T)[];
  loop?: boolean | undefined;
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

export function checkbox<T>(config: CheckboxConfig<Choice<T>>): Promise<T[]> {
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore type error in jest?!?
  return checkboxFn(config);
}
