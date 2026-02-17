#!/usr/bin/env node

// Browser launcher for integration tests
// Spawns headless Chromium and communicates via IPC

const { chromium } = require('playwright');
const path = require('path');
const readline = require('readline');
const http = require('http');
const fs = require('fs');

let browser = null;
let page = null;
let server = null;

async function launch() {
    console.error('Starting HTTP server...');

    // Start simple HTTP server to serve files
    const rootDir = path.join(__dirname, '../..');
    server = http.createServer((req, res) => {
        console.error('Request:', req.url);
        let filePath = path.join(rootDir, req.url);
        console.error('Trying:', filePath);
        if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
            console.error('Found:', filePath);
            const ext = path.extname(filePath);
            const contentTypes = {
                '.html': 'text/html',
                '.js': 'application/javascript',
                '.wasm': 'application/wasm',
            };
            res.writeHead(200, {
                'Content-Type': contentTypes[ext] || 'text/plain',
                'Cross-Origin-Opener-Policy': 'same-origin',
                'Cross-Origin-Embedder-Policy': 'require-corp'
            });
            fs.createReadStream(filePath).pipe(res);
        } else {
            res.writeHead(404);
            res.end('Not found');
        }
    });

    await new Promise(resolve => server.listen(0, () => {
        console.error('Server listening on port', server.address().port);
        resolve();
    }));

    const serverPort = server.address().port;

    console.error('Launching browser...');
    browser = await chromium.launch({ headless: true });
    page = await browser.newPage();

    page.on('console', msg => console.error('Browser console:', msg.text()));
    page.on('pageerror', err => console.error('Browser error:', err));

    const url = `http://localhost:${serverPort}/tests/integration/browser-test-page.html`;
    console.error('Loading page from:', url);
    await page.goto(url);
    console.error('Page loaded');

    // Wait for API to be ready
    await page.waitForFunction(() => window.testAPI !== undefined, { timeout: 10000 });
    console.error('Test API ready');

    sendIPC({ type: 'ready' });
}

function sendIPC(msg) {
    console.log('IPC:' + JSON.stringify(msg));
}

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false
});

rl.on('line', async (line) => {
    try {
        const msg = JSON.parse(line);

        switch (msg.cmd) {
            case 'connect':
                await page.evaluate(async ({ wsUrl, subscriptions }) => {
                    await window.testAPI.connect(wsUrl, subscriptions);
                }, { wsUrl: msg.wsUrl, subscriptions: msg.subscriptions });
                sendIPC({ type: 'connected' });
                break;

            case 'setPath':
                await page.evaluate(({ path, value }) => {
                    window.testAPI.setPath(path, value);
                }, { path: msg.path, value: msg.value });
                sendIPC({ type: 'set_complete' });
                break;

            case 'getPath':
                const value = await page.evaluate(({ path }) => {
                    return window.testAPI.getPath(path);
                }, { path: msg.path });
                sendIPC({ type: 'value', value });
                break;

            case 'waitForBroadcast':
                try {
                    await page.evaluate(async () => {
                        await window.testAPI.waitForBroadcast();
                    });
                    sendIPC({ type: 'broadcast_received' });
                } catch (err) {
                    sendIPC({ type: 'error', error: err.message });
                }
                break;

            case 'close':
                await page.evaluate(() => window.testAPI.close());
                await browser.close();
                if (server) server.close();
                process.exit(0);
                break;

            default:
                sendIPC({ type: 'error', error: `Unknown command: ${msg.cmd}` });
        }
    } catch (err) {
        sendIPC({ type: 'error', error: err.message, stack: err.stack });
    }
});

// Start launcher
launch().catch(err => {
    console.error('Failed to launch browser:', err);
    process.exit(1);
});
