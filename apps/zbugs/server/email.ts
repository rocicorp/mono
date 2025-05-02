import nodemailer from 'nodemailer';

async function getTransport() {
  if (process.env.NODE_ENV === 'development') {
    if (!process.env.LOOPS_EMAIL_API_KEY) {
      throw new Error('LOOPS_API_KEY is not set');
    }
    const transport = nodemailer.createTransport({
      host: 'smtp.loops.so',
      name: 'loops',
      port: 587,
      secure: false,
      auth: {
        user: 'loops',
        pass: process.env.LOOPS_EMAIL_API_KEY,
      },
    });

    (transport as any).isLoops = true;
    return transport;
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

  if ((transport as any).isLoops) {
    await transport.sendMail({
      from: 'no-reply@roci.dev',
      to: recipients.join(', '),
      subject: title,
      text: JSON.stringify({
        transactionalId: process.env.LOOPS_TRANSACTIONAL_ID,
        email: recipients.join(', '),
        dataVariables: {
          subject: title,
          message: message,
          link: link,
        },
      }),
    });
  } else {
    await transport.sendMail({
      from: 'no-reply@roci.dev',
      to: recipients.join(', '),
      subject: title,
      text: `${message}\n\n${link}`,
    });
  }
}
