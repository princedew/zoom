import { randomUUID } from "node:crypto";
import { WebSocket, WebSocketServer } from "ws";

import type { DtlsParameters } from "mediasoup/node/lib/types";

import {
  closePeerTransports,
  connectPeerTransport,
  createPeerProducer,
  createPeerTransport,
  getRouterRtpCapabilities,
  getTransportParameters,
  initializeMedia,
} from "./media.js";

type RpcRequest = {
  type: string;
  requestId: string;
  data?: unknown;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function parseRequest(value: unknown): RpcRequest {
  if (
    !isRecord(value) ||
    typeof value.type !== "string" ||
    typeof value.requestId !== "string"
  ) {
    throw new Error("Invalid request format");
  }

  return {
    type: value.type,
    requestId: value.requestId,
    data: value.data,
  };
}

function send(ws: WebSocket, message: Record<string, unknown>): void {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(message));
  }
}

function sendError(ws: WebSocket, requestId: string, error: unknown): void {
  send(ws, {
    type: "ERROR",
    requestId,
    error:
      error instanceof Error
        ? error.message
        : "An unexpected server error occurred",
  });
}

async function main(): Promise<void> {
  await initializeMedia();

  const wss = new WebSocketServer({ port: 8000 });

  // Each room ID maps to the WebSocket connections in that room.
  const rooms = new Map<string, Set<WebSocket>>();

  // Track which room each connection belongs to.
  const peerRooms = new Map<WebSocket, string>();

  console.log("WebSocket server running on ws://localhost:8000");

  wss.on("connection", (ws: WebSocket) => {
    const peerId = randomUUID();

    console.log("Client connected:", peerId);

    send(ws, {
      type: "CONNECTED",
      message: "Welcome! You are connected to the server.",
    });

    ws.on("message", async (rawMessage) => {
      let request: RpcRequest;

      try {
        const parsed: unknown = JSON.parse(rawMessage.toString());
        request = parseRequest(parsed);
      } catch (error: unknown) {
        send(ws, {
          type: "ERROR",
          error:
            error instanceof Error
              ? error.message
              : "Invalid JSON or request format",
        });

        return;
      }

      console.log("Request received:", request.type);

      try {
        // Return the SFU Router's RTP capabilities.
        if (request.type === "GET_ROUTER_RTP_CAPABILITIES") {
          send(ws, {
            type: "ROUTER_RTP_CAPABILITIES",
            requestId: request.requestId,
            data: getRouterRtpCapabilities(),
          });

          return;
        }

        // Create a server-side WebRTC transport for this peer.
        if (request.type === "CREATE_WEBRTC_TRANSPORT") {
          const transport = await createPeerTransport(peerId);

          send(ws, {
            type: "WEBRTC_TRANSPORT_CREATED",
            requestId: request.requestId,
            data: getTransportParameters(transport),
          });

          return;
        }

        // Connect the server-side transport using browser DTLS parameters.
        if (request.type === "TRANSPORT_CONNECT") {
          if (
            !isRecord(request.data) ||
            typeof request.data.transportId !== "string" ||
            !isRecord(request.data.dtlsParameters)
          ) {
            throw new Error("Invalid TRANSPORT_CONNECT data");
          }

          const transportId = request.data.transportId;
          const dtlsParameters = request.data
            .dtlsParameters as unknown as DtlsParameters;

          await connectPeerTransport(peerId, transportId, dtlsParameters);

          send(ws, {
            type: "TRANSPORT_CONNECTED",
            requestId: request.requestId,
            data: { connected: true },
          });

          return;
        }

        if (request.type === "TRANSPORT_PRODUCE") {
          if (
            !isRecord(request.data) ||
            typeof request.data.transportId !== "string" ||
            (request.data.kind !== "audio" && request.data.kind !== "video") ||
            !isRecord(request.data.rtpParameters)
          ) {
            throw new Error("Invalid TRANSPORT_PRODUCE data");
          }

          const producer = await createPeerProducer(
            peerId,
            request.data.transportId,
            request.data.kind,
            request.data.rtpParameters as unknown as RtpParameters,
          );

          send(ws, {
            type: "TRANSPORT_PRODUCER_CREATED",
            requestId: request.requestId,
            data: { id: producer.id },
          });

          return;
        }

        // Create a room and make this peer its first member.
        if (request.type === "CREATE_ROOM") {
          if (peerRooms.has(ws)) {
            throw new Error("Leave your current room first");
          }

          const roomId = randomUUID();

          rooms.set(roomId, new Set([ws]));
          peerRooms.set(ws, roomId);

          send(ws, {
            type: "ROOM_CREATED",
            requestId: request.requestId,
            data: { roomId },
          });

          return;
        }

        // Join an existing room.
        if (request.type === "JOIN_ROOM") {
          if (
            !isRecord(request.data) ||
            typeof request.data.roomId !== "string"
          ) {
            throw new Error("A valid roomId is required");
          }

          if (peerRooms.has(ws)) {
            throw new Error("You are already in a room");
          }

          const roomId = request.data.roomId;
          const room = rooms.get(roomId);

          if (!room) {
            throw new Error("Room not found");
          }

          room.add(ws);
          peerRooms.set(ws, roomId);

          send(ws, {
            type: "ROOM_JOINED",
            requestId: request.requestId,
            data: {
              roomId,
              participantCount: room.size,
            },
          });

          return;
        }

        // Basic RPC health check.
        if (request.type === "PING") {
          send(ws, {
            type: "PONG",
            requestId: request.requestId,
            data: { message: "Server is alive" },
          });

          return;
        }

        throw new Error(`Unknown request type: ${request.type}`);
      } catch (error: unknown) {
        console.error(`${request.type} failed:`, error);
        sendError(ws, request.requestId, error);
      }
    });

    ws.on("close", () => {
      // Release this peer's mediasoup resources.
      closePeerTransports(peerId);

      // Remove this peer from its room.
      const roomId = peerRooms.get(ws);

      if (roomId) {
        const room = rooms.get(roomId);

        room?.delete(ws);

        if (room?.size === 0) {
          rooms.delete(roomId);
        }

        peerRooms.delete(ws);
      }

      console.log("Client disconnected:", peerId);
    });

    ws.on("error", (error: Error) => {
      console.error("WebSocket error:", error);
    });
  });
}

main().catch((error: unknown) => {
  console.error("Server startup failed:", error);
  process.exitCode = 1;
});
