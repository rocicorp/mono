/**
 * Checks if we can create HTML elements and are in a browser document context
 */
function canUseHTMLDialog(): boolean {
  try {
    // Check if we're in a test environment (vitest sets this)
    if (
      typeof globalThis !== 'undefined' &&
      '__vitest_worker__' in globalThis
    ) {
      return false;
    }

    return (
      typeof document !== 'undefined' &&
      typeof document.createElement === 'function' &&
      typeof HTMLDialogElement !== 'undefined' &&
      document.body !== null &&
      // Make sure we can actually create a dialog element
      document.createElement('dialog') instanceof HTMLDialogElement
    );
  } catch {
    return false;
  }
}

/**
 * Creates a password prompt using HTML <dialog> element
 */
export function createHTMLPasswordPrompt(
  message: string,
): Promise<string | null> {
  if (!canUseHTMLDialog()) {
    // Fallback to browser prompt if HTML dialog is not available
    return Promise.resolve(prompt(message));
  }

  return new Promise<string | null>(resolve => {
    const allRevertStyle = 'all:revert;';
    const dialog = document.createElement('dialog');
    dialog.style.cssText = `${allRevertStyle}padding: 1em; border: 1px solid; border-radius: 4px;`;

    // Prevent keydown from escaping the dialog which can be interfered by other
    // listeners (e.g. global hotkeys)
    dialog.addEventListener('keydown', e => {
      e.stopPropagation();
    });

    dialog.oncancel = () => {
      dialog.remove();
      resolve(null);
    };

    const form = document.createElement('form');
    form.method = 'dialog';
    form.style.cssText = allRevertStyle;

    const messagePara = document.createElement('p');
    messagePara.style.cssText = allRevertStyle;
    messagePara.append(message);

    const passwordInput = document.createElement('input');
    passwordInput.type = 'password';
    passwordInput.placeholder = 'Admin password';
    passwordInput.autocomplete = 'current-password';
    passwordInput.autofocus = true;
    passwordInput.style.cssText = `${allRevertStyle}display: block; margin: 0.5em 0;`;

    const buttonDiv = document.createElement('div');
    buttonDiv.style.cssText = allRevertStyle;

    const cancelBtn = document.createElement('button');
    cancelBtn.type = 'reset';
    cancelBtn.append('Cancel');
    cancelBtn.style.cssText = `${allRevertStyle}margin-right: 0.5em;`;

    const okBtn = document.createElement('button');
    okBtn.type = 'submit';
    okBtn.value = 'ok';
    okBtn.append('OK');
    okBtn.style.cssText = allRevertStyle;

    buttonDiv.append(cancelBtn, okBtn);
    form.append(messagePara, passwordInput, buttonDiv);
    dialog.append(form);

    form.onreset = () => {
      dialog.close();
    };

    dialog.onclose = () => {
      // debugger;
      if (dialog.returnValue === 'ok') {
        resolve(passwordInput.value || null);
      } else {
        resolve(null);
      }
      dialog.remove();
    };

    document.body.append(dialog);
    dialog.showModal();
  });
}
