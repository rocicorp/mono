export const reactComments = [
  {
    comment_id: '1102947110',
    created_at: '2022-04-19 18:15:53',
    updated_at: '2022-04-19 18:15:53',
    body: 'I was able to resolve this issue with two package.json changes:\r\n  \r\n1. Specified the latest React 17 version: according to my build server 17.0.44 was the last successful build.\r\n2. Added optionalDependencies anything greater than 17.0.44 is optional.  This setting help resolved the typescript build issues.\r\n\r\n"devDependencies": {\r\n        "@types/react": "17.0.44"\r\n },\r\n "optionalDependencies": {\r\n        "@types/react": "^17.0.44"\r\n  },',
    number: 24304,
    creator_user_login: 'jsarelas',
  },
  {
    comment_id: '1103200095',
    created_at: '2022-04-19 21:41:17',
    updated_at: '2022-04-19 21:41:17',
    body: "Seeing this on React Native. Not sure what to do. It started happening after I tried to remote debug just one time, now it's stuck that way.",
    number: 23202,
    creator_user_login: 'shamilovtim',
  },
];
