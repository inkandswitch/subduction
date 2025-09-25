# Subduction Wasm bindings

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
