declare module '*issues-react.js.gz' {
  const gitHubIssues: {
    number: number;
    title: string;
    body: string | null;
    state: 'open' | 'closed';
    // eslint-disable-next-line @typescript-eslint/naming-convention
    updated_at: string;
    // eslint-disable-next-line @typescript-eslint/naming-convention
    created_at: string;
    // eslint-disable-next-line @typescript-eslint/naming-convention
    creator_user_login: string;
  }[];
  export default gitHubIssues;
}
