# AST to ZQL

This is a simple tool that outputs a ZQL query from an AST.

## Usage

Save the AST to a json file. Then run the following command:

### With Bun

```bash
cat ast.json | bun ast-to-zql.ts
```

### With Deno

```bash
cat ast.json | deno run --allow-sys ast-to-zql.ts
```

### With NodeJS

````bash
cat ast.json | node --experimental-strip-types --no-warnings ast-to-zql.ts```
````
