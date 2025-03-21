# AST to ZQL

This is a simple tool that outputs a ZQL query from an AST.

## Usage

Save the AST to a json file. Then run the following command:

```bash
cat ast.json | npx ast-to-zql
```

or if you have a schema file you will get the name mapping applied using the
following command:

```bash
cat ast.json | npx ast-to-zql --schema schema.ts
```
