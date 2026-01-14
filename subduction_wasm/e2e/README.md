# Subduction WASM E2E Tests

Comprehensive end-to-end Playwright tests for the subduction_wasm module.

## Test Suites

### 1. Core Functionality Tests (`subduction.spec.ts`)
Tests the WASM bindings without network dependencies.

**Coverage:**
- Constructor and initialization (2 tests)
- Storage operations (1 test)
- Sedimentree management (4 tests)
- Data retrieval (4 tests)
- Event handling (4 tests)
- Multiple instances (1 test)
- Type system (3 tests)
- Error handling (3 tests)
- API smoke tests (1 test)

**Total: 23 tests × 3 browsers = 69 tests**

### 2. Peer Connection Tests (`peer-connection.spec.ts`)
Tests WebSocket connections and sync operations with actual peers using `subduction_cli`.

**Coverage:**
- WebSocket server connection
- Connection registration with Subduction
- Using `SubductionWebSocket.connect`
- Disconnecting from peers
- Requesting blobs from connected peers
- Multiple concurrent connections

**Total: 6 tests on chromium only**

## Prerequisites

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
# Run all core tests across all browsers
npx playwright test --grep-invert "@peer"

# Run peer connection tests (chromium only)
npx playwright test --grep "@peer" --project=chromium
```

### Run Specific Test Files
```bash
# Core functionality tests only
npx playwright test subduction.spec.ts

# Peer connection tests only
npx playwright test peer-connection.spec.ts --project=chromium
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

## Important Notes

### Peer Connection Tests
- **Serial execution:** Peer connection tests run serially to avoid port conflicts
- **Single browser:** Only run on chromium to prevent multiple servers on same port
- **Server lifecycle:** Tests start `subduction_cli` in `beforeAll` and stop it in `afterAll`
- **Port:** WebSocket server runs on `127.0.0.1:9892` (different from http-server on 9891)

### Cleaning Up Stale Processes
If peer connection tests fail with "Address already in use", kill stale processes:
```bash
# Check for running subduction_cli
ps aux | grep subduction_cli

# Kill all subduction_cli processes
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
