# Subduction Wasm bindings

🚧 This is an early release preview. It has a very unstable API. No guarantees are given. DO NOT use for production use cases at this time. USE AT YOUR OWN RISK. 🚧

## Build package

```
wasm-pack build --target web --out-dir pkg
```

## Run tests

Install dependencies:
```
pnpm install
```

Install Playwright's browser binaries:
```
npx playwright install
```

Run tests:
```
npx playwright test
```

View Playwright report:
```
npx playwright show-report
```

## Naming Conventions

`wasm-bindgen`-generated types ownership rules can be confusing and cumbersome, with different behavior depending on which side of the Wasm boundary your type was created from. To help keep this straight, we adopt the naming convention:

> [!NOTE]
> Types exported from Rust are prefixed with `Wasm`.
> Types imported from JS are prefixed with `Js`.
