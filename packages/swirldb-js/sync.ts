/**
 * SwirlDB Sync Client
 *
 * Handles real-time synchronization with upstream server using binary WebSocket protocol.
 * Automatically reconnects and batches changes for optimal performance.
 */

import type { SwirlDB } from './index';

// Message type constants (must match server)
const MSG_CONNECT = 0x01;
const MSG_SYNC = 0x02;
const MSG_PUSH = 0x03;
const MSG_BROADCAST = 0x04;
const MSG_PUSH_ACK = 0x05;
const MSG_PING = 0x10;
const MSG_PONG = 0x11;
const MSG_ERROR = 0xFF;

export interface SyncConfig {
  upstreamUrl: string;
  clientId: string;
  roomId: string;
  reconnectDelayMs?: number;
  maxReconnectAttempts?: number;
  batchChanges?: boolean;
  batchDelayMs?: number;
  debugMode?: boolean; // Include human-readable JSON for network inspection
}

export class SyncManager {
  private config: Required<SyncConfig>;
  private ws: WebSocket | null = null;
  private db: SwirlDB;
  private connected = false;
  private reconnectAttempts = 0;
  private reconnectTimeout: any = null;
  private pendingChanges: Uint8Array[] = [];
  private batchTimeout: any = null;

  constructor(db: SwirlDB, config: SyncConfig) {
    this.db = db;
    this.config = {
      reconnectDelayMs: 1000,
      maxReconnectAttempts: 10,
      batchChanges: true,
      batchDelayMs: 100,
      debugMode: false,
      ...config
    };

    // Check for global debug flags
    // 1. URL parameter: ?swirldb_debug=true
    // 2. window.SWIRLDB_DEBUG = true (set in console or app code)
    // 3. localStorage.getItem('swirldb-debug-mode') === 'true'
    if (typeof window !== 'undefined') {
      const urlParams = new URLSearchParams(window.location.search);
      const urlDebug = urlParams.get('swirldb_debug') === 'true';
      const windowDebug = (window as any).SWIRLDB_DEBUG === true;
      const storageDebug = localStorage.getItem('swirldb-debug-mode') === 'true';

      if (urlDebug || windowDebug || storageDebug) {
        this.config.debugMode = true;
        console.log(`🐛 [DEBUG] Debug mode enabled via ${urlDebug ? 'URL param' : windowDebug ? 'window.SWIRLDB_DEBUG' : 'localStorage'}`);
      }
    }

    if (this.config.debugMode) {
      console.log(`🐛 [DEBUG] SwirlDB Sync initialized with debug mode enabled`);
      console.log(`   → Binary messages will include human-readable annotations`);
      console.log(`   → Open browser DevTools → Network tab to inspect traffic`);
      console.log(`   → Disable: syncManager.disableDebugMode()`);
    }
  }

  /**
   * Connect to upstream server
   */
  async connect(): Promise<void> {
    if (this.ws) {
      throw new Error('Already connected');
    }

    return new Promise((resolve, reject) => {
      console.log(`🔌 Connecting to upstream: ${this.config.upstreamUrl}`);

      this.ws = new WebSocket(this.config.upstreamUrl);
      this.ws.binaryType = 'arraybuffer';

      this.ws.onopen = () => {
        console.log('✅ Connected to upstream');
        this.connected = true;
        this.reconnectAttempts = 0;
        this.sendConnect();
        resolve();
      };

      this.ws.onmessage = (event) => {
        this.handleMessage(new Uint8Array(event.data));
      };

      this.ws.onclose = () => {
        console.log('👋 Disconnected from upstream');
        this.connected = false;
        this.ws = null;
        this.scheduleReconnect();
      };

      this.ws.onerror = (error) => {
        console.error('❌ WebSocket error:', error);
        reject(error);
      };
    });
  }

  /**
   * Disconnect from upstream server
   */
  disconnect(): void {
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }

    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }

    this.connected = false;
  }

  /**
   * Check if connected to upstream
   */
  isConnected(): boolean {
    return this.connected;
  }

  /**
   * Enable debug mode (adds human-readable annotations to network traffic)
   */
  enableDebugMode(): void {
    this.config.debugMode = true;
    console.log(`🐛 [DEBUG] Debug mode ENABLED`);
    console.log(`   → Binary messages will include human-readable annotations`);
    console.log(`   → Open browser DevTools → Network tab to inspect traffic`);
  }

  /**
   * Disable debug mode (production binary-only protocol)
   */
  disableDebugMode(): void {
    this.config.debugMode = false;
    console.log(`🐛 [DEBUG] Debug mode DISABLED (production mode)`);
  }

  /**
   * Check if debug mode is enabled
   */
  isDebugMode(): boolean {
    return this.config.debugMode;
  }

  /**
   * Push local changes to upstream (with automatic batching)
   */
  pushChanges(changes: Uint8Array[]): void {
    if (changes.length === 0) return;

    this.pendingChanges.push(...changes);

    if (this.config.batchChanges) {
      // Debounce: wait for more changes before sending
      if (this.batchTimeout) {
        clearTimeout(this.batchTimeout);
      }

      this.batchTimeout = setTimeout(() => {
        this.flushPendingChanges();
      }, this.config.batchDelayMs);
    } else {
      // Send immediately
      this.flushPendingChanges();
    }
  }

  private flushPendingChanges(): void {
    if (this.pendingChanges.length === 0 || !this.connected || !this.ws) {
      return;
    }

    const changes = this.pendingChanges;
    this.pendingChanges = [];

    console.log(`📤 Pushing ${changes.length} change(s) to upstream`);

    const message = this.encodePush(this.config.roomId, changes);

    if (this.config.debugMode) {
      const debugInfo = {
        _debug: 'SwirlDB Debug Mode - This message is for human inspection only',
        type: 'MSG_PUSH',
        type_code: '0x03',
        room_id: this.config.roomId,
        changes_count: changes.length,
        changes_sizes: changes.map(c => c.length + ' bytes'),
        total_binary_size: message.length + ' bytes',
        note: 'Actual changes are in the preceding binary frame',
        timestamp: new Date().toISOString()
      };
      console.log(`🐛 [DEBUG] → MSG_PUSH (0x03)`, debugInfo);

      // Send JSON as text frame for network inspection
      this.ws.send(JSON.stringify(debugInfo));
    }

    this.ws.send(message);
  }

  private sendConnect(): void {
    if (!this.ws) return;

    console.log(`🔌 Sending Connect message for room: ${this.config.roomId}`);

    // For now, we don't track heads client-side (Automerge will handle merging)
    // In the future, we can optimize by sending heads to avoid re-syncing known changes
    const message = this.encodeConnect(
      this.config.clientId,
      this.config.roomId,
      new Uint8Array()
    );

    if (this.config.debugMode) {
      const debugInfo = {
        _debug: 'SwirlDB Debug Mode - This message is for human inspection only',
        type: 'MSG_CONNECT',
        type_code: '0x01',
        client_id: this.config.clientId,
        room_id: this.config.roomId,
        heads_length: 0,
        binary_size: message.length + ' bytes',
        timestamp: new Date().toISOString()
      };
      console.log(`🐛 [DEBUG] → MSG_CONNECT (0x01)`, debugInfo);

      // Send JSON as text frame for network inspection
      this.ws.send(JSON.stringify(debugInfo));
    }

    this.ws.send(message);
  }

  private handleMessage(buffer: Uint8Array): void {
    const msgType = buffer[0];

    if (this.config.debugMode) {
      const msgTypeNames: Record<number, string> = {
        [MSG_SYNC]: 'MSG_SYNC',
        [MSG_BROADCAST]: 'MSG_BROADCAST',
        [MSG_PUSH_ACK]: 'MSG_PUSH_ACK',
        [MSG_PING]: 'MSG_PING',
        [MSG_PONG]: 'MSG_PONG',
        [MSG_ERROR]: 'MSG_ERROR'
      };
      const msgTypeName = msgTypeNames[msgType] || 'UNKNOWN';
      const debugInfo = {
        _debug: 'SwirlDB Debug Mode - Received from server',
        direction: 'INCOMING',
        type: msgTypeName,
        type_code: '0x' + msgType.toString(16).padStart(2, '0'),
        binary_size: buffer.length + ' bytes',
        timestamp: new Date().toISOString()
      };
      console.log(`🐛 [DEBUG] ← ${msgTypeName} (0x${msgType.toString(16).padStart(2, '0')})`, debugInfo);
    }

    switch (msgType) {
      case MSG_SYNC:
        this.handleSync(buffer);
        break;

      case MSG_BROADCAST:
        this.handleBroadcast(buffer);
        break;

      case MSG_PUSH_ACK:
        console.log('✅ Push acknowledged by upstream');
        break;

      case MSG_PONG:
        // Heartbeat response
        if (this.config.debugMode) {
          console.log(`🐛 [DEBUG]    Heartbeat pong received`);
        }
        break;

      case MSG_ERROR:
        this.handleError(buffer);
        break;

      default:
        console.warn(`Unknown message type: 0x${msgType.toString(16)}`);
    }
  }

  private handleSync(buffer: Uint8Array): void {
    const { changes } = this.decodeChanges(buffer, 1);

    if (changes.length === 0) {
      console.log('📥 No changes from upstream (already in sync)');
      return;
    }

    console.log(`📥 Received ${changes.length} change(s) from upstream`);

    if (this.config.debugMode) {
      console.log(`🐛 [DEBUG]    Sync details:`, {
        changes_count: changes.length,
        changes_sizes: changes.map(c => c.length + ' bytes'),
        note: 'Changes are binary Automerge CRDT operations'
      });
    }

    // Apply changes to local database
    // TODO: This requires exposing Automerge's applyChanges in the SwirlDB API
    // For now, we'll log them
    console.log('TODO: Apply changes to local DB');
  }

  private handleBroadcast(buffer: Uint8Array): void {
    let offset = 1;

    // Parse from_client_id
    const clientIdLen = readUint32(buffer, offset);
    offset += 4;
    const fromClientId = new TextDecoder().decode(buffer.slice(offset, offset + clientIdLen));
    offset += clientIdLen;

    const { changes } = this.decodeChanges(buffer, offset);

    console.log(`📣 Received broadcast from ${fromClientId}: ${changes.length} change(s)`);

    if (this.config.debugMode) {
      console.log(`🐛 [DEBUG]    Broadcast details:`, {
        from_client_id: fromClientId,
        changes_count: changes.length,
        changes_sizes: changes.map(c => c.length + ' bytes'),
        note: 'Real-time update from peer client'
      });
    }

    // Apply changes to local database
    // TODO: Implement applyChanges
    console.log('TODO: Apply broadcasted changes to local DB');
  }

  private handleError(buffer: Uint8Array): void {
    const errorMsgLen = readUint32(buffer, 1);
    const errorMsg = new TextDecoder().decode(buffer.slice(5, 5 + errorMsgLen));
    console.error(`❌ Server error: ${errorMsg}`);
  }

  private scheduleReconnect(): void {
    if (this.reconnectAttempts >= this.config.maxReconnectAttempts) {
      console.error('❌ Max reconnect attempts reached, giving up');
      return;
    }

    this.reconnectAttempts++;
    const delay = this.config.reconnectDelayMs * Math.pow(2, this.reconnectAttempts - 1); // Exponential backoff

    console.log(`🔄 Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${this.config.maxReconnectAttempts})`);

    this.reconnectTimeout = setTimeout(() => {
      this.connect().catch((error) => {
        console.error('❌ Reconnect failed:', error);
      });
    }, delay);
  }

  // Encoding functions (binary protocol)
  private encodeConnect(clientId: string, roomId: string, heads: Uint8Array): Uint8Array {
    const clientIdBytes = new TextEncoder().encode(clientId);
    const roomIdBytes = new TextEncoder().encode(roomId);

    const size = 1 + 4 + clientIdBytes.length + 4 + roomIdBytes.length + 4 + heads.length;
    const buffer = new Uint8Array(size);
    let offset = 0;

    buffer[offset++] = MSG_CONNECT;
    writeUint32(buffer, offset, clientIdBytes.length);
    offset += 4;
    buffer.set(clientIdBytes, offset);
    offset += clientIdBytes.length;
    writeUint32(buffer, offset, roomIdBytes.length);
    offset += 4;
    buffer.set(roomIdBytes, offset);
    offset += roomIdBytes.length;
    writeUint32(buffer, offset, heads.length);
    offset += 4;
    buffer.set(heads, offset);

    return buffer;
  }

  private encodePush(roomId: string, changes: Uint8Array[]): Uint8Array {
    const roomIdBytes = new TextEncoder().encode(roomId);

    let size = 1 + 4 + roomIdBytes.length + 4;
    for (const change of changes) {
      size += 4 + change.length;
    }

    const buffer = new Uint8Array(size);
    let offset = 0;

    buffer[offset++] = MSG_PUSH;
    writeUint32(buffer, offset, roomIdBytes.length);
    offset += 4;
    buffer.set(roomIdBytes, offset);
    offset += roomIdBytes.length;
    writeUint32(buffer, offset, changes.length);
    offset += 4;

    for (const change of changes) {
      writeUint32(buffer, offset, change.length);
      offset += 4;
      buffer.set(change, offset);
      offset += change.length;
    }

    return buffer;
  }

  private decodeChanges(buffer: Uint8Array, offset: number): { changes: Uint8Array[], offset: number } {
    const changesCount = readUint32(buffer, offset);
    offset += 4;

    const changes: Uint8Array[] = [];
    for (let i = 0; i < changesCount; i++) {
      const changeLen = readUint32(buffer, offset);
      offset += 4;
      const change = buffer.slice(offset, offset + changeLen);
      offset += changeLen;
      changes.push(change);
    }

    return { changes, offset };
  }
}

// Binary helpers
function readUint32(buffer: Uint8Array, offset: number): number {
  return (buffer[offset] << 24) |
         (buffer[offset + 1] << 16) |
         (buffer[offset + 2] << 8) |
         buffer[offset + 3];
}

function writeUint32(buffer: Uint8Array, offset: number, value: number): void {
  buffer[offset] = (value >>> 24) & 0xFF;
  buffer[offset + 1] = (value >>> 16) & 0xFF;
  buffer[offset + 2] = (value >>> 8) & 0xFF;
  buffer[offset + 3] = value & 0xFF;
}
