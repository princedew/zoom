import { createServer } from "node:http";
import { WebSocketServer, WebSocket } from "ws";
import * as mediasoup from "mediasoup";
import type {
  Worker,
  Router,
  WebRtcTransport,
  Producer,
  Consumer,
} from "mediasoup/types";

type RequestMessage = {
  id: string;
  method: string;
  data?: Record<string, any>;
};

type Peer = {
  ws: WebSocket;
  transports: Map<string, WebRtcTransport>;
  producers: Map<string, Producer>;
  consumers: Map<string, Consumer>;
};

const peers = new Map<WebSocket, Peer>();

let worker: Worker;
let router: Router;

function send(ws: WebSocket, message: object): void {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(message));
  }
}

function notifyOthers(sender: WebSocket, message: object): void {
  for (const [ws] of peers) {
    if (ws !== sender) send(ws, message);
  }
}

async function createTransport() {
  return router.createWebRtcTransport({
    listenInfos: [
      {
        protocol: "udp",
        ip: "127.0.0.1",
      },
    ],
    enableTcp: true,
    enableUdp: true,
    preferUdp: true,
  });
}

async function main() {
  worker = await mediasoup.createWorker();
  router = await worker.createRouter({
    mediaCodecs: [
      {
        kind: "audio",
        mimeType: "audio/opus", // ??
        clockRate: 48000, // ??
        channels: 2, // ??
      },
      {
        kind: "video",
        mimeType: "video/VP8",
        clockRate: 90000,
      },
    ],
  });
}

const server = createServer();
const wss = new WebSocketServer({ server });
wss.on("connection", (ws) => {
    const peer: Peer = {
      ws,
      transports: new Map(),
      producers: new Map(),
      consumers: new Map(),
    };


    peers.set(ws, peer);

    
});
