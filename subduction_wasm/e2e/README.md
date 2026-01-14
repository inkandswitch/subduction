# Subduction WASM E2E Tests

End-to-end Playwright tests for `subduction_wasm`.

## Setup

1. **Build the WASM package:**

```bash
cd subduction_wasm
pnpm install
pnpm build
```

2. **Build subduction_cli (for peer connection tests):**

```bash
cd ..  # to subduction root
cargo build --release -p subduction_cli
```

3. **Install Playwright browsers:**

```bash
cd subduction_wasm
npx playwright install
```

## Running Tests

### Run All Tests

```bash
npx playwright test
```

### Run Specific Test Files

```bash
# Core functionality tests only
npx playwright test subduction.spec.ts

# Peer connection tests only (all browsers)
npx playwright test peer-connection.spec.ts
```

### Run with UI

```bash
npx playwright test --ui
```

### Run in Headed Mode (see browser)

```bash
npx playwright test --headed
```

### Run Specific Browser

```bash
npx playwright test --project=chromium
npx playwright test --project=firefox
npx playwright test --project=webkit
```

### View Test Report

```bash
npx playwright show-report
```

## Notes

### Peer Connection Tests

- **Serial execution:** Peer connection tests run serially within each browser to avoid connection conflicts
- **Multi-browser support:** Each browser runs its own WebSocket server on a different port
  - Chromium: 9892 (WebSocket) + 6669 (console_subscriber)
  - Firefox: 9893 (WebSocket) + 6670 (console_subscriber)
  - WebKit: 9894 (WebSocket) + 6671 (console_subscriber)
- **Server lifecycle:** Tests start `subduction_cli` in `beforeAll` and stop it in `afterAll`
- **Port configuration:** Uses `TOKIO_CONSOLE_BIND` environment variable to assign different console_subscriber ports per browser, preventing port conflicts when running tests in parallel

### Cleaning Up Stale Processes

If peer connection tests fail with "Address already in use", kill stale processes:

```bash
ps aux | grep subduction_cli
pkill -9 subduction_cli
```

### Debugging

```bash
# Run with debug output
DEBUG=pw:api npx playwright test

# Run single test with trace
npx playwright test --trace on --grep "should connect to WebSocket server"

# Open trace viewer
npx playwright show-trace trace.zip
```

## Test Architecture

### Core Tests
```
Browser → http-server (port 9891) → index.html → pkg-slim (WASM)
  ↓
page.evaluate() → window.subduction → WASM API
  ↓
Assertions in Node.js test runner
```

### Peer Connection Tests
```
Node.js Test → spawn(subduction_cli) → WebSocket Server (port 9892)
  ↓
Browser → http-server (port 9891) → index.html → pkg-slim (WASM)
  ↓
page.evaluate() → SubductionWebSocket.connect(ws://127.0.0.1:9892)
  ↓
WASM Client ←WebSocket→ Rust Server (subduction_cli)
  ↓
Assertions verify actual peer sync operations
```

## Configuration Files

- `playwright.config.ts` - Playwright test configuration
- `e2e/config.ts` - Test URL configuration
- `e2e/server/index.html` - Test HTML page that loads WASM
- `e2e/server/pkg/` - Symlink to `pkg-slim` (base64-encoded WASM)

## Troubleshooting

### Tests hang or timeout
- Check if http-server is accessible: `curl http://localhost:9891`
- Check if WebSocket server is running: `lsof -i :9892`
- Ensure pkg-slim is built: `ls e2e/server/pkg/`

### WASM not loading
- Rebuild WASM: `pnpm build`
- Check symlink: `ls -la e2e/server/pkg`
- Should point to `../../pkg-slim`

### Server startup timeout
- Check for port conflicts: `lsof -i :9892`
- Kill stale processes: `pkill subduction_cli`
- Increase timeout in test file if needed

## Development Workflow

1. Make changes to Rust code
2. Rebuild WASM: `pnpm build`
3. Run tests: `npx playwright test`
4. View failures: `npx playwright show-report`
5. Debug specific test: `npx playwright test --debug --grep "test name"`
