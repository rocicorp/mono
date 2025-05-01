import nodemailer from 'nodemailer';

async function getTransport() {
  if (process.env.NODE_ENV === 'production') {
    // we do not have a production transport configured yet
    return undefined;
  }

  const testAccount = await nodemailer.createTestAccount();
  console.log('MAILER TEST ACCOUNT:', testAccount.user, testAccount.pass);
  return nodemailer.createTransport({
    host: 'smtp.ethereal.email',
    port: 587,
    secure: false,
    auth: {
      user: testAccount.user,
      pass: testAccount.pass,
    },
  });
}

let transport: Awaited<ReturnType<typeof getTransport>> | undefined;
export async function sendEmail({
  recipients,
  title,
  message,
  link,
}: {
  recipients: string[];
  title: string;
  message: string;
  link: string;
}) {
  if (!transport) {
    transport = await getTransport();
    if (!transport) {
      console.log('No email transport configured');
      return;
    }
  }

  console.log('emailing', recipients);
  await transport.sendMail({
    from: 'no-replay@roci.dev',
    to: recipients.join(', '),
    subject: title,
    text: `${message}\n\n${link}`,
  });
}
