import * as net from 'net';
import {
  HEADER_SIZE,
  OpMsgMessage,
  buildOpMsg,
  parseOpMsg,
  nextReqId,
  OP_MSG,
} from './wire_protocol';

export interface ConnectionOptions {
  host: string;
  port: number;
  connectTimeoutMs?: number;
  socketTimeoutMs?: number;
}

export class Connection {
  private socket: net.Socket | null = null;
  private pendingRequests = new Map<
    number,
    { resolve: (msg: OpMsgMessage) => void; reject: (err: Error) => void }
  >();
  private readBuffer = Buffer.alloc(0);
  private connected = false;
  private closed = false;
  private options: Required<ConnectionOptions>;

  constructor(opts: ConnectionOptions) {
    this.options = {
      host: opts.host,
      port: opts.port,
      connectTimeoutMs: opts.connectTimeoutMs ?? 10_000,
      socketTimeoutMs: opts.socketTimeoutMs ?? 0,
    };
  }

  connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.connected) return resolve();

      const sock = new net.Socket();
      this.socket = sock;

      const timer = setTimeout(() => {
        sock.destroy();
        reject(new Error(`Connection timed out after ${this.options.connectTimeoutMs}ms`));
      }, this.options.connectTimeoutMs);

      sock.connect(this.options.port, this.options.host, () => {
        clearTimeout(timer);
        this.connected = true;
        resolve();
      });

      sock.once('error', (err) => {
        clearTimeout(timer);
        this.connected = false;
        reject(err);
      });

      sock.on('data', (chunk: Buffer) => {
        this.onData(chunk);
      });

      sock.once('close', () => {
        this.connected = false;
        this.closed = true;
        for (const [, pending] of this.pendingRequests) {
          pending.reject(new Error('Connection closed'));
        }
        this.pendingRequests.clear();
      });
    });
  }

  private onData(chunk: Buffer): void {
    this.readBuffer = Buffer.concat([this.readBuffer, chunk]);

    while (this.readBuffer.length >= HEADER_SIZE) {
      const msgLen = this.readBuffer.readInt32LE(0);
      if (msgLen < HEADER_SIZE) {
        this.readBuffer = Buffer.alloc(0);
        return;
      }
      if (this.readBuffer.length < msgLen) return; // incomplete

      const msgBuf = this.readBuffer.subarray(0, msgLen);
      this.readBuffer = this.readBuffer.subarray(msgLen);

      try {
        const msg = parseOpMsg(msgBuf);
        const pending = this.pendingRequests.get(msg.responseTo);
        if (pending) {
          this.pendingRequests.delete(msg.responseTo);
          pending.resolve(msg);
        }
      } catch (err) {
        // swallow parse errors on incoming; could emit
      }
    }
  }

  async send(msg: OpMsgMessage): Promise<OpMsgMessage> {
    if (!this.connected || !this.socket) {
      throw new Error('Not connected');
    }
    return new Promise<OpMsgMessage>((resolve, reject) => {
      const reqId = msg.requestID;
      this.pendingRequests.set(reqId, { resolve, reject });

      const buf = buildOpMsg(msg);
      this.socket!.write(buf, (err) => {
        if (err) {
          this.pendingRequests.delete(reqId);
          reject(err);
        }
      });
    });
  }

  close(): void {
    if (this.socket && !this.closed) {
      this.socket.destroy();
      this.socket = null;
      this.connected = false;
      this.closed = true;
    }
  }

  get isConnected(): boolean {
    return this.connected;
  }
}
