import { test as base, expect } from '@playwright/test';
import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import * as net from 'node:net';
import { URL as pageURL } from './config';

export const test = base.extend<{}, { serverAddress: string }>({
  page: async ({ page }, use) => {
    await page.goto(pageURL);
    await page.waitForFunction(() => window.subductionReady);
    await use(page);
  },
  serverAddress: [async ({}, use) => {
    const reservation = net.createServer();
    await new Promise<void>(resolve => reservation.listen(0, '127.0.0.1', resolve));
    const port = (reservation.address() as net.AddressInfo).port;
    await new Promise<void>((resolve, reject) => reservation.close(error => error ? reject(error) : resolve()));
    const release = fileURLToPath(new URL('../../target/release/subduction_cli', import.meta.url));
    const debug = fileURLToPath(new URL('../../target/debug/subduction_cli', import.meta.url));
    const cli = process.env.SUBDUCTION_CLI ?? (existsSync(release) ? release : debug);
    const child = spawn(cli, ['server', '--socket', `127.0.0.1:${port}`, '--ephemeral-key', '--auth', 'open'], { stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, TOKIO_CONSOLE_BIND: '127.0.0.1:0' } });
    let logs = '';
    let spawnError: Error | undefined;
    child.once('error', error => { spawnError = error; });
    child.stderr.on('data', bytes => { logs += bytes; });
    child.stdout.on('data', bytes => { logs += bytes; });
    const exited = new Promise<void>(resolve => child.once('exit', () => resolve()));
    try {
      const deadline = Date.now() + 15000;
      for (;;) {
        if (spawnError) throw spawnError;
        if (child.exitCode !== null) throw new Error(`server exited: ${logs}`);
        const listening = await new Promise<boolean>(resolve => {
          const socket = net.connect(port, '127.0.0.1');
          socket.once('connect', () => { socket.destroy(); resolve(true); });
          socket.once('error', () => { socket.destroy(); resolve(false); });
        });
        if (listening) break;
        if (Date.now() > deadline) throw new Error(`server failed to listen: ${logs}`);
        await new Promise(resolve => setTimeout(resolve, 50));
      }
      await use(`127.0.0.1:${port}`);
    } finally {
      child.kill('SIGTERM');
      const force = setTimeout(() => child.kill('SIGKILL'), 3000);
      force.unref();
      if (!spawnError) await exited;
      clearTimeout(force);
    }
  }, { scope: 'worker' }],
});
export { expect };
