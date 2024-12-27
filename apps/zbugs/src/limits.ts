// NOTE: These limits are also in the postgres schema. These here are just for
// UI feedback.
export const MAX_ISSUE_TITLE_LENGTH = 128;
export const MAX_ISSUE_DESCRIPTION_LENGTH = 10 * 1024;

// The launch post has a special maxLength because trolls.
export const maxCommentLength = (issueID: string) =>
  issueID === 'duuW9Nyj5cTNLlimp9Qje' ? 1024 : 64 * 1024;
