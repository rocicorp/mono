import nodemailer from 'nodemailer';

async function getTransport() {
  if (process.env.LOOPS_EMAIL_API_KEY !== undefined) {
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
  } else {
    const testAccount = await nodemailer.createTestAccount();
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
}

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
  const transport = await getTransport();
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
