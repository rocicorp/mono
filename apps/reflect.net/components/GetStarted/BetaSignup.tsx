import styles from './BetaSignup.module.css';
import {useState, useRef} from 'react';
import {event} from 'nextjs-google-analytics';

export default function BetaSignup() {
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [message, setMessage] = useState('');
  const [buttonText, setButtonText] = useState('Join Waitlist');
  const form = useRef<HTMLFormElement>(null);

  const handleSubmit = async (e: React.SyntheticEvent) => {
    e.preventDefault();

    setButtonText('Submitting...');

    const data = form.current;
    if (data === null) return;
    const formData = new FormData(data);
    const endpoint = '/api/submit-beta-form';
    const options = {
      method: 'POST',
      body: JSON.stringify(Object.fromEntries(formData)),
    };

    const response = await fetch(endpoint, options);
    // check if response is 200 else display an error
    if (response.ok) {
      // display success message
      console.log('Beta request successfully sent.');
      setButtonText('Received');
      setName('');
      setEmail('');
      setMessage('');
      setTimeout(() => {
        setButtonText('Join Waitlist');
      }, 3000);
      event('beta_request_submitted', {
        category: 'Get started',
        action: 'Press join waitlist button',
        label: 'Conversion',
      });
    } else {
      // display error message
      console.log('error');
      setButtonText('Try Again');
    }
  };

  return (
    <div className={styles.formContainer}>
      <form ref={form} onSubmit={handleSubmit}>
        <div className={styles.inputContainer}>
          <label className={styles.formLabel} htmlFor="name">
            Full name
          </label>
          <input
            className={styles.textField}
            value={name}
            onChange={e => setName(e.target.value)}
            type="text"
            id="name"
            name="name"
            placeholder=""
          />
        </div>
        <div className={styles.inputContainer}>
          <label className={styles.formLabel} htmlFor="email">
            Email
          </label>
          <input
            className={styles.textField}
            value={email}
            onChange={e => setEmail(e.target.value)}
            type="email"
            id="email"
            name="email"
            placeholder=""
          />
        </div>
        <div className={styles.inputContainer}>
          <label className={styles.formLabel} htmlFor="message">
            Message <span className={styles.optional}>optional</span>
          </label>
          <textarea
            className={styles.textArea}
            value={message}
            onChange={e => setMessage(e.target.value)}
            id="message"
            name="message"
            placeholder=""
          />
        </div>
        <div className={styles.ctaWrap}>
          <button
            className={styles.buttonPrimary}
            disabled={!name || !email}
            type="submit"
          >
            {buttonText}
          </button>
        </div>
      </form>
    </div>
  );
}
