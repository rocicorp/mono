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
    // Shared CSS constants
    const reset = 'all:revert;';
    const w = 'rgba(255,255,255,';
    const white = w + '1)';
    const whiteTransp = w + '0.4)';
    const r1 = '0.25rem';
    const font = `font-family:system-ui,sans-serif;color:${white};`;
    const btnBase = `${reset}${font}cursor:pointer;font-size:1rem;font-weight:500;border:none;padding:0.4rem 0.75rem;border-radius:${r1};background:`;

    const dialog = document.createElement('dialog');
    dialog.style.cssText = `${reset}${font}background:rgba(0,0,0,0.95);padding:2rem;border:1px solid ${whiteTransp};border-radius:0.5rem;`;

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
    form.style.cssText = `${reset}margin:0;`;

    const messagePara = document.createElement('p');
    messagePara.style.cssText = `${reset}${font}font-size:1.5rem;margin:0 0 1rem 0;`;
    messagePara.append(message);

    const passwordInput = document.createElement('input');
    passwordInput.type = 'password';
    passwordInput.placeholder = 'Admin password';
    passwordInput.autocomplete = 'current-password';
    passwordInput.autofocus = true;
    passwordInput.style.cssText = `${reset}${font}font-size:1rem;display:block;margin:0 0 1rem 0;padding:0.5rem;background:rgba(0,0,0,0.5);border:1px solid ${whiteTransp};border-radius:${r1};`;

    const buttonDiv = document.createElement('div');
    buttonDiv.style.cssText = reset;

    const cancelBtn = document.createElement('button');
    cancelBtn.type = 'reset';
    cancelBtn.append('Cancel');
    cancelBtn.style.cssText = btnBase + w + '0.25);';

    const okBtn = document.createElement('button');
    okBtn.type = 'submit';
    okBtn.value = 'ok';
    okBtn.append('OK');
    okBtn.style.cssText = btnBase + 'rgba(19,106,235,1);margin-right:0.5rem;';

    buttonDiv.append(okBtn, cancelBtn);
    form.append(messagePara, passwordInput, buttonDiv);
    dialog.append(form);

    form.onreset = () => {
      dialog.close();
    };

    dialog.onclose = () => {
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
