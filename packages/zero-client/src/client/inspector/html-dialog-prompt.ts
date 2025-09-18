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
    // Create dialog element
    const dialog = document.createElement('dialog');
    dialog.style.cssText =
      'all: revert; padding: 1em; border: 1px solid; border-radius: 4px;';

    // Create form
    const form = document.createElement('form');
    form.style.cssText = 'all: revert;';

    // Create message paragraph
    const messagePara = document.createElement('p');
    messagePara.style.cssText = 'all: revert;';
    messagePara.textContent = message;

    // Create password input
    const passwordInput = document.createElement('input');
    passwordInput.type = 'password';
    passwordInput.placeholder = 'Admin password';
    passwordInput.autocomplete = 'current-password';
    passwordInput.style.cssText =
      'all: revert; display: block; margin: 0.5em 0;';

    // Create button container
    const buttonDiv = document.createElement('div');
    buttonDiv.style.cssText = 'all: revert;';

    // Create Cancel button
    const cancelBtn = document.createElement('button');
    cancelBtn.type = 'button';
    cancelBtn.textContent = 'Cancel';
    cancelBtn.style.cssText = 'all: revert; margin-right: 0.5em;';

    // Create OK button
    const okBtn = document.createElement('button');
    okBtn.type = 'submit';
    okBtn.textContent = 'OK';
    okBtn.style.cssText = 'all: revert;';

    // Assemble the DOM structure
    buttonDiv.appendChild(cancelBtn);
    buttonDiv.appendChild(okBtn);
    form.appendChild(messagePara);
    form.appendChild(passwordInput);
    form.appendChild(buttonDiv);
    dialog.appendChild(form);

    // Handle form submission (OK button)
    form.onsubmit = e => {
      e.preventDefault();
      const password = passwordInput.value;
      document.body.removeChild(dialog);
      resolve(password || null);
    };

    // Handle Cancel button click
    cancelBtn.onclick = () => {
      document.body.removeChild(dialog);
      resolve(null);
    };

    // Handle ESC key to cancel
    dialog.oncancel = () => {
      document.body.removeChild(dialog);
      resolve(null);
    };

    // Add dialog to document and show it
    document.body.appendChild(dialog);
    dialog.showModal();

    // Focus the password input
    passwordInput.focus();
  });
}
