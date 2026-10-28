import * as mediasoup from "mediasoup";

import type {
  DtlsParameters,
  Producer,
  RtpParameters,
  Router,
  Worker,
  WebRtcTransport,
} from "mediasoup/node/lib/types";


const peerProducers = new Map<string, Map<string, Producer>>();

let worker: Worker | undefined;
let router: Router | undefined;

const peerTransports = new Map<string, Map<string, WebRtcTransport>>();

export async function initializeMedia(): Promise<Router> {
  worker = await mediasoup.createWorker({
    logLevel: "warn",
    rtcMinPort: 40000,
    rtcMaxPort: 40100,
  });

  worker.on("died", (error: Error): void => {
    console.error("mediasoup worker died:", error);
    process.exit(1);
  });

  router = await worker.createRouter({
    mediaCodecs: [
      {
        kind: "audio",
        mimeType: "audio/opus",
        clockRate: 48000,
        channels: 2,
      },
      {
        kind: "video",
        mimeType: "video/VP8",
        clockRate: 90000,
      },
    ],
  });

  console.log("mediasoup router initialized");

  return router;
}

export function getRouter(): Router {
  if (!router) {
    throw new Error("Media router has not been initialized");
  }else{
    return router;
  }
}

export function getRouterRtpCapabilities() {
  return getRouter().rtpCapabilities;
}

export async function createWebRtcTransport(): Promise<WebRtcTransport> {
  return getRouter().createWebRtcTransport({
    listenInfos: [
      {
        protocol: "udp",
        ip: "0.0.0.0",
        announcedAddress: "127.0.0.1",
      },
      {
        protocol: "tcp",
        ip: "0.0.0.0",
        announcedAddress: "127.0.0.1",
      },
    ],
    enableUdp: true,
    enableTcp: true,
  });
}

export async function createPeerTransport(
  peerId: string,
): Promise<WebRtcTransport> {
  const transport = await createWebRtcTransport();

  let transports = peerTransports.get(peerId);

  if (!transports) {
    transports = new Map<string, WebRtcTransport>();
    peerTransports.set(peerId, transports);
  }

  // Store the actual transport under its own ID.
  transports.set(transport.id, transport);

  transport.on("close", () => {
    transports.delete(transport.id);

    if (transports.size === 0) {
      peerTransports.delete(peerId);
    }
  });

  return transport;
}

export function getTransportParameters(transport: WebRtcTransport) {
  return {
    id: transport.id,
    iceParameters: transport.iceParameters,
    iceCandidates: transport.iceCandidates,
    dtlsParameters: transport.dtlsParameters,
  };
}

export function getPeerTransport(
  peerId: string,
  transportId: string,
): WebRtcTransport {
  const transport = peerTransports.get(peerId)?.get(transportId);

  if (!transport) {
    throw new Error("WebRTC transport not found for this peer");
  }

  return transport;
}

export async function connectPeerTransport(
  peerId: string,
  transportId: string,
  dtlsParameters: DtlsParameters,
): Promise<void> {
  const transport = getPeerTransport(peerId, transportId);

  await transport.connect({ dtlsParameters });
}

export function closePeerTransports(peerId: string): void {
  const producers = peerProducers.get(peerId);

  if (producers) {
    peerProducers.delete(peerId);

    for (const producer of producers.values()) {
      producer.close();
    }
  }

  const transports = peerTransports.get(peerId);

  if (!transports) return;

  peerTransports.delete(peerId);

  for (const transport of transports.values()) {
    transport.close();
  }

  transports.clear();
}

export async function createPeerProducer(
  peerId: string,
  transportId: string,
  kind: "audio" | "video",
  rtpParameters: RtpParameters,
): Promise<Producer> {
  const transport = getPeerTransport(peerId, transportId);

  const producer = await transport.produce({
    kind,
    rtpParameters,
  });

  let producers = peerProducers.get(peerId);

  if (!producers) {
    producers = new Map<string, Producer>();
    peerProducers.set(peerId, producers);
  }

  producers.set(producer.id, producer);

  producer.on("transportclose", () => {
    producers?.delete(producer.id);

    if (producers?.size === 0) {
      peerProducers.delete(peerId);
    }
  });

  return producer;
}

