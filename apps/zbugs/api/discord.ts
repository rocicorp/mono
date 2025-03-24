export async function postToDiscord({
  title,
  message,
  link,
}: {
  title: string;
  message: string;
  link: string;
}) {
  const content = `**${title}**\n${message}\n<${link}>`;
  const url = process.env.DISCORD_WEBHOOK_URL;
  if (!url) {
    console.log('No discord URL, skipping posting message', content);
    return;
  }

  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({content}),
    });

    if (Math.floor(response.status / 100) !== 2) {
      console.error(
        'Failed to post to Discord:',
        response,
        await response.text(),
      );
    }
  } catch (e) {
    console.error('Failed to post to Discord:', e);
  }
}
