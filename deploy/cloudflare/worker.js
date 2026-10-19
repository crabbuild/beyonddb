import { DurableObject } from 'cloudflare:workers';

// Exactly three explicitly named database nodes; no request-driven random instances.
export class BeyondNode extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.monitoring = false;
    if (ctx.container?.running) this.watch();
  }
  watch() {
    if (this.monitoring) return;
    this.monitoring = true;
    this.ctx.waitUntil(this.ctx.container.monitor().then(
      () => this.ctx.storage.put('lastStop', { at: Date.now(), outcome: 'stopped' }),
      error => this.ctx.storage.put('lastStop', { at: Date.now(), outcome: 'failed', message: error.message, exitCode: error.exitCode }),
    ).finally(() => { this.monitoring = false; }));
  }
  async start(index, bootstrap = false) {
    const c = this.ctx.container;
    if (!c.running) {
      c.start({
        image: c.images.base,
        instance: { vcpu: 1, memoryMib: 3072, diskMb: 5000 },
        enableInternet: true,
        env: {
          NODE_INDEX: String(index),
          BOOTSTRAP: bootstrap ? '1' : '0',
          WORKER_URL: this.env.WORKER_URL,
          R2_ENDPOINT: this.env.R2_ENDPOINT,
          R2_BUCKET: this.env.R2_BUCKET,
          AWS_ACCESS_KEY_ID: this.env.R2_ACCESS_KEY_ID,
          AWS_SECRET_ACCESS_KEY: this.env.R2_SECRET_ACCESS_KEY,
          AWS_REGION: 'auto',
          PEER_KEY_HEX: this.env.PEER_KEY_HEX,
          ENCRYPTION_KEY_HEX: this.env.ENCRYPTION_KEY_HEX,
          DDB_ACCESS_KEY: this.env.DDB_ACCESS_KEY,
          DDB_SECRET_KEY: this.env.DDB_SECRET_KEY,
          TUNNEL_TOKEN: this.env.TUNNEL_TOKEN,
        },
        labels: { benchmark: 'beyonddb-perf', node: String(index) },
      });
      await c.setInactivityTimeout(60 * 60 * 1000);
    }
    this.watch();
    return { running: c.running, info: await c.inspect() };
  }
  async fetch(request) {
    if (new URL(request.url).pathname.startsWith('/_peer/')) return this.tunnel();
    return this.publicFetch(request);
  }
  async publicFetch(request) {
    const upstream = new Request(request.url.replace(/^https:/, 'http:'), request);
    return this.ctx.container.getTcpPort(8080).fetch(upstream);
  }
  async inspectNode() {
    const c = this.ctx.container;
    if (!c.running) return { running: false, lastStop: await this.ctx.storage.get('lastStop') };
    const response = await c.getTcpPort(8090).fetch('http://agent/status');
    return { running: c.running, info: await c.inspect(), agent: await response.json() };
  }
  async logs() {
    const response = await this.ctx.container.getTcpPort(8090).fetch('http://agent/logs');
    return new Response(response.body, { headers: { 'content-type': 'text/plain' } });
  }
  async stop() {
    if (!this.ctx.container.running) return { running: false };
    this.ctx.container.signal(15);
    return { stopping: true };
  }
  async tunnel() {
    // The public Worker validates the independent tunnel secret before this RPC.
    const socket = this.ctx.container.getTcpPort(9000).connect('10.0.0.1:9000');
    await socket.opened;
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    server.binaryType = 'arraybuffer';
    const writer = socket.writable.getWriter();
    let writes = Promise.resolve();
    let closed = false;
    const close = () => {
      if (closed) return;
      closed = true;
      socket.close();
      try { server.close(1000); } catch {}
    };
    server.addEventListener('message', event => {
      writes = writes.then(async () => {
        if (typeof event.data === 'string') throw new Error('Peer frames must be binary');
        const buffer = event.data instanceof Blob ? await event.data.arrayBuffer() : event.data;
        await writer.write(new Uint8Array(buffer));
      }).catch(error => { console.error('Peer writer failed', error.message); close(); });
    });
    server.addEventListener('close', close);
    server.addEventListener('error', close);
    this.ctx.waitUntil((async () => {
      try {
        const reader = socket.readable.getReader();
        while (!closed) {
          const { value, done } = await reader.read();
          if (done) break;
          server.send(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength));
        }
      } catch (error) { console.error('Peer reader failed', error.message); } finally { close(); }
    })());
    return new Response(null, { status: 101, webSocket: client });
  }
}

function authorized(request, token) {
  return typeof token === 'string' && token.length >= 32 &&
    request.headers.get('authorization') === `Bearer ${token}`;
}
function node(env, index) {
  if (!Number.isInteger(index) || index < 1 || index > 3) throw new Error('Invalid node');
  return env.NODES.get(env.NODES.idFromName(`node-${index}`), { locationHint: 'wnam' });
}
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const peer = url.pathname.match(/^\/_peer\/([1-3])$/);
    if (peer) {
      if (!authorized(request, env.TUNNEL_TOKEN)) return new Response('Forbidden', { status: 403 });
      if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') return new Response('WebSocket required', { status: 400 });
      return node(env, Number(peer[1])).fetch(request);
    }
    const admin = url.pathname.match(/^\/_admin\/([1-3])\/(start|status|logs|stop)$/);
    if (admin) {
      if (!authorized(request, env.ADMIN_TOKEN)) return new Response('Forbidden', { status: 403 });
      const index = Number(admin[1]), target = node(env, index);
      if (admin[2] === 'start' && request.method === 'POST') return Response.json(await target.start(index, index === 1 && url.searchParams.get('bootstrap') === '1'));
      if (admin[2] === 'stop' && request.method === 'POST') return Response.json(await target.stop());
      if (admin[2] === 'status' && request.method === 'GET') return Response.json(await target.inspectNode());
      if (admin[2] === 'logs' && request.method === 'GET') return target.logs();
      return new Response('Method not allowed', { status: 405 });
    }
    if (url.pathname !== '/' && url.pathname !== '/health') return new Response('Not found', { status: 404 });
    const index = Number(request.headers.get('x-beyonddb-node') || '1');
    if (![1, 2, 3].includes(index)) return new Response('Invalid ingress node', { status: 400 });
    return node(env, index).fetch(request);
  },
};
