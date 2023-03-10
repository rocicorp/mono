## Reflect.net

## Basics

Copy the .env files

`cp .env.example .env; cp .dev.vars.example .dev.vars`

In separate terminals, run:

`npm run dev-worker`

`npm start`

## Renderer

To rebuild the renderer wasm module, you'll need some prerequisites:

[Rust](https://doc.rust-lang.org/book/ch01-01-installation.html):

`curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | sh`

A C compiler:

`xcode-select --install`

[wasm-pack](https://rustwasm.github.io/wasm-pack/installer/)

`curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh`
