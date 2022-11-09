export default function files(codeSnippets, template) {
  return codeSnippets.reduce((result, codeSnippet) => {
    if (codeSnippet.props.mdxType !== 'pre') {
      return result;
    }

    const {props} = codeSnippet.props.children;
    let filePath; // path in the folder structure
    let fileHidden = false; // if the file is available as a tab
    let fileActive = false; // if the file tab is shown by default

    if (props.metastring) {
      const [name, ...params] = props.metastring.split(' ');
      filePath = '/' + name;
      if (params.includes('hidden')) {
        fileHidden = true;
      }
      if (params.includes('active')) {
        fileActive = true;
      }
    } else {
      if (props.className === 'language-js') {
        filePath = '/App.js';
      } else if (props.className === 'language-ts') {
        filePath = '/App.tsx';
      } else if (props.className === 'language-tsx') {
        filePath = '/App.tsx';
      } else if (props.className === 'language-css') {
        filePath = '/styles.css';
      } else {
        throw new Error(`Code block is missing a filename: ${props.children}`);
      }
    }
    if (result[filePath]) {
      throw new Error(
        `File ${filePath} was defined multiple times. Each file snippet should have a unique path name`,
      );
    }
    result[filePath] = {
      code: props.children,
      hidden: fileHidden,
      active: fileActive,
    };

    return result;
  }, {});
}
