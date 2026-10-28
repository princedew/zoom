"use client";

import { Device } from "mediasoup-client";
import { useEffect, useRef, useState } from "react";

type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (reason: Error) => void;
  timeoutId: ReturnType<typeof setTimeout>;
};

type RpcResponse = {
  type?: string;
  requestId?: string;
  data?: unknown;
  error?: string;
  message?: string;
};

type RoomCreatedData = {
  roomId: string;
};

type RoomJoinedData = {
  roomId: string;
  participantCount: number;
};

type MediasoupDevice = Awaited<ReturnType<typeof Device.factory>>;

type MediasoupSendTransport = ReturnType<
  MediasoupDevice["createSendTransport"]
>;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function parseRpcResponse(value: unknown): RpcResponse {
  if (!isRecord(value)) {
    throw new Error("Invalid server response");
  }

  const response: RpcResponse = {};

  if (typeof value.type === "string") response.type = value.type;
  if (typeof value.requestId === "string") {
    response.requestId = value.requestId;
  }
  if ("data" in value) response.data = value.data;
  if (typeof value.error === "string") response.error = value.error;
  if (typeof value.message === "string") {
    response.message = value.message;
  }

  return response;
}

function parseRoomCreatedData(value: unknown): RoomCreatedData {
  if (!isRecord(value) || typeof value.roomId !== "string") {
    throw new Error("Invalid room creation response");
  }

  return { roomId: value.roomId };
}

function parseRoomJoinedData(value: unknown): RoomJoinedData {
  if (
    !isRecord(value) ||
    typeof value.roomId !== "string" ||
    typeof value.participantCount !== "number"
  ) {
    throw new Error("Invalid room join response");
  }

  return {
    roomId: value.roomId,
    participantCount: value.participantCount,
  };
}

const Vr = () => {
  const [status, setStatus] = useState("Connecting...");
  const [serverMessage, setServerMessage] = useState("");
  const [roomId, setRoomId] = useState("");
  const [roomMessage, setRoomMessage] = useState("");

  const socketRef = useRef<WebSocket | null>(null);
  const pendingRequests = useRef(new Map<string, PendingRequest>());

  const deviceRef = useRef<MediasoupDevice | null>(null);
  const sendTransportRef = useRef<MediasoupSendTransport | null>(null);

  const localVideoRef = useRef<HTMLVideoElement | null>(null);
  const localStreamRef = useRef<MediaStream | null>(null);

  // Stop local media when this component unmounts.
  useEffect(() => {
    return () => {
      localStreamRef.current?.getTracks().forEach((track) => track.stop());
      localStreamRef.current = null;

      if (localVideoRef.current) {
        localVideoRef.current.srcObject = null;
        localVideoRef.current.onloadedmetadata = null;
        localVideoRef.current.onplaying = null;
        localVideoRef.current.onerror = null;
      }
    };
  }, []);

  // Establish the WebSocket connection.
  useEffect(() => {
    const ws = new WebSocket("ws://localhost:8000");

    socketRef.current = ws;

    ws.onopen = () => {
      if (socketRef.current !== ws) return;
      setStatus("Connected");
      console.log("WebSocket connected");
    };

    ws.onmessage = (event: MessageEvent) => {
      if (socketRef.current !== ws) return;

      let response: RpcResponse;

      try {
        const parsed: unknown = JSON.parse(String(event.data));
        response = parseRpcResponse(parsed);
      } catch (error: unknown) {
        console.error("Invalid server message:", error);
        return;
      }

      console.log("Message from server:", response);

      if (!response.requestId) {
        setServerMessage(response.message ?? "");
        return;
      }

      const pending = pendingRequests.current.get(response.requestId);

      if (!pending) return;

      clearTimeout(pending.timeoutId);
      pendingRequests.current.delete(response.requestId);

      if (response.error) {
        pending.reject(new Error(response.error));
      } else {
        pending.resolve(response.data);
      }
    };

    ws.onerror = (event: Event) => {
      if (socketRef.current !== ws) return;

      console.error("WebSocket error:", event);
      setStatus("Connection error");
    };

    ws.onclose = () => {
      if (socketRef.current !== ws) return;

      socketRef.current = null;
      setStatus("Disconnected");

      for (const pending of pendingRequests.current.values()) {
        clearTimeout(pending.timeoutId);
        pending.reject(new Error("WebSocket disconnected"));
      }

      pendingRequests.current.clear();

      sendTransportRef.current?.close();
      sendTransportRef.current = null;
      deviceRef.current = null;
    };

    return () => {
      if (socketRef.current === ws) {
        socketRef.current = null;

        for (const pending of pendingRequests.current.values()) {
          clearTimeout(pending.timeoutId);
          pending.reject(new Error("WebSocket disconnected"));
        }

        pendingRequests.current.clear();

        sendTransportRef.current?.close();
        sendTransportRef.current = null;
        deviceRef.current = null;
      }

      // Avoid logging a misleading error for intentional cleanup.
      ws.onopen = null;
      ws.onmessage = null;
      ws.onerror = null;
      ws.onclose = null;
      ws.close();
    };
  }, []);

  // RPC helper.
  function rpc(
    type: string,
    data: Record<string, unknown> = {},
  ): Promise<unknown> {
    const socket = socketRef.current;

    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return Promise.reject(new Error("WebSocket is not connected"));
    }

    const requestId = crypto.randomUUID();

    return new Promise<unknown>((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        pendingRequests.current.delete(requestId);
        reject(new Error(`Request timed out: ${type}`));
      }, 10_000);

      pendingRequests.current.set(requestId, {
        resolve,
        reject,
        timeoutId,
      });

      try {
        socket.send(JSON.stringify({ type, requestId, data }));
      } catch (error: unknown) {
        clearTimeout(timeoutId);
        pendingRequests.current.delete(requestId);

        reject(
          error instanceof Error
            ? error
            : new Error("Failed to send RPC request"),
        );
      }
    });
  }

  async function pingServer(): Promise<void> {
    try {
      const result = await rpc("PING");

      if (!isRecord(result) || typeof result.message !== "string") {
        throw new Error("Invalid PING response");
      }

      setServerMessage(result.message);
    } catch (error: unknown) {
      console.error("PING failed:", error);
      setServerMessage(
        error instanceof Error ? error.message : "Request failed",
      );
    }
  }

  async function createRoom(): Promise<void> {
    try {
      const result = parseRoomCreatedData(await rpc("CREATE_ROOM"));

      setRoomId(result.roomId);
      setRoomMessage("Room created successfully");
    } catch (error: unknown) {
      console.error("Create room failed:", error);
      setRoomMessage(
        error instanceof Error ? error.message : "Failed to create room",
      );
    }
  }

  async function joinRoom(): Promise<void> {
    try {
      const result = parseRoomJoinedData(await rpc("JOIN_ROOM", { roomId }));

      setRoomId(result.roomId);
      setRoomMessage(`Joined room. Participants: ${result.participantCount}`);
    } catch (error: unknown) {
      console.error("Join room failed:", error);
      setRoomMessage(
        error instanceof Error ? error.message : "Failed to join room",
      );
    }
  }

  async function setupMediaTransport(): Promise<void> {
    if (sendTransportRef.current) {
      setRoomMessage("A send transport already exists");
      return;
    }

    try {
      const routerRtpCapabilities = (await rpc(
        "GET_ROUTER_RTP_CAPABILITIES",
      )) as Parameters<MediasoupDevice["load"]>[0]["routerRtpCapabilities"];

      const device = await Device.factory();

      await device.load({ routerRtpCapabilities });

      deviceRef.current = device;

      console.log("Device loaded:", device.loaded);
      console.log("Can produce audio:", device.canProduce("audio"));
      console.log("Can produce video:", device.canProduce("video"));

      const transportOptions = (await rpc(
        "CREATE_WEBRTC_TRANSPORT",
      )) as Parameters<MediasoupDevice["createSendTransport"]>[0];

      const sendTransport = device.createSendTransport(transportOptions);

      sendTransport.on(
        "connect",
        async ({ dtlsParameters }, callback, errback) => {
          try {
            await rpc("TRANSPORT_CONNECT", {
              transportId: sendTransport.id,
              dtlsParameters,
            });

            callback();
            console.log("Server accepted DTLS parameters");
          } catch (error: unknown) {
            errback(
              error instanceof Error
                ? error
                : new Error("Failed to connect transport"),
            );
          }
        },
      );

      sendTransport.on(
        "produce",
        async ({ kind, rtpParameters, appData }, callback, errback) => {
          try {
            const result: unknown = await rpc("TRANSPORT_PRODUCE", {
              transportId: sendTransport.id,
              kind,
              rtpParameters,
              appData,
            });

            if (!isRecord(result) || typeof result.id !== "string") {
              throw new Error("Invalid producer response");
            }

            callback({ id: result.id });

            console.log(`${kind} producer created:`, result.id);
          } catch (error: unknown) {
            errback(
              error instanceof Error
                ? error
                : new Error("Failed to create producer"),
            );
          }
        },
      );

      sendTransport.on("connectionstatechange", (connectionState) => {
        console.log("WebRTC connection state:", connectionState);
        setRoomMessage(`WebRTC connection: ${connectionState}`);
      });

      sendTransportRef.current = sendTransport;

      setRoomMessage("Device loaded; send transport created");
      console.log("Send transport created:", sendTransport.id);
    } catch (error: unknown) {
      console.error("Media setup failed:", error);

      setRoomMessage(
        error instanceof Error
          ? error.message
          : "Failed to initialize media transport",
      );

      sendTransportRef.current?.close();
      sendTransportRef.current = null;
      deviceRef.current = null;
    }
  }

  // Capture camera/microphone, show local preview, and publish tracks.
  async function startCameraAndMic(): Promise<void> {
    const device = deviceRef.current;
    const sendTransport = sendTransportRef.current;

    if (!device || !sendTransport) {
      setRoomMessage("Set up the media transport first");
      return;
    }

    if (localStreamRef.current) {
      setRoomMessage("Camera and microphone are already running");
      return;
    }

    let stream: MediaStream | null = null;

    try {
      if (!navigator.mediaDevices?.getUserMedia) {
        throw new Error("Camera access is unavailable in this browser context");
      }

      console.log("Requesting camera and microphone permission...");

      stream = await navigator.mediaDevices.getUserMedia({
        audio: true,
        video: {
          facingMode: "user",
          width: { ideal: 1280 },
          height: { ideal: 720 },
        },
      });

      // Diagnose the actual media tracks.
      const videoTrack = stream.getVideoTracks()[0];

      console.log("Stream tracks:", {
        active: stream.active,
        audio: stream.getAudioTracks().length,
        video: stream.getVideoTracks().length,
      });

      console.log("Camera track details:", {
        exists: Boolean(videoTrack),
        label: videoTrack?.label,
        enabled: videoTrack?.enabled,
        muted: videoTrack?.muted,
        readyState: videoTrack?.readyState,
        settings: videoTrack?.getSettings(),
      });

      if (!videoTrack) {
        throw new Error("No video track was returned by the camera");
      }

      videoTrack.addEventListener("mute", () => {
        console.warn("Camera track muted");
      });

      videoTrack.addEventListener("unmute", () => {
        console.log("Camera track unmuted");
      });

      videoTrack.addEventListener("ended", () => {
        console.warn("Camera track ended");
      });

      // Attach the stream to the local video element.
      const videoElement = localVideoRef.current;

      if (!videoElement) {
        throw new Error("Local video element was not found");
      }

      localStreamRef.current = stream;
      videoElement.srcObject = stream;

      videoElement.onloadedmetadata = () => {
        console.log("Video metadata loaded:", {
          width: videoElement.videoWidth,
          height: videoElement.videoHeight,
          readyState: videoElement.readyState,
        });
      };

      videoElement.onplaying = () => {
        console.log("Local video is playing");
      };

      videoElement.onerror = () => {
        console.error("Video element error:", videoElement.error);
      };

      try {
        await videoElement.play();
        console.log("Video playback started successfully");

        if (videoElement) {
          const checkFrames = () => {
            const video = videoElement;

            console.log("Video frame diagnostics:", {
              videoWidth: video.videoWidth,
              videoHeight: video.videoHeight,
              paused: video.paused,
              currentTime: video.currentTime,
              readyState: video.readyState,
              totalVideoFrames:
                video.getVideoPlaybackQuality?.().totalVideoFrames ??
                "Unavailable",
            });
          };

          checkFrames();

          const intervalId = window.setInterval(checkFrames, 2000);

          videoElement.onended = () => {
            window.clearInterval(intervalId);
          };
        }
      } catch (error: unknown) {
        console.error("Video playback failed:", error);
        throw error;
      }

      // Publish each track to mediasoup.
      for (const track of stream.getTracks()) {
        await sendTransport.produce({ track });
        console.log(`Started producing ${track.kind}`);
      }

      setRoomMessage(
        "Local camera preview active; media tracks published to the SFU",
      );
    } catch (error: unknown) {
      // Stop media if startup fails.
      stream?.getTracks().forEach((track) => track.stop());

      if (localVideoRef.current) {
        localVideoRef.current.srcObject = null;
        localVideoRef.current.onloadedmetadata = null;
        localVideoRef.current.onplaying = null;
        localVideoRef.current.onerror = null;
      }

      localStreamRef.current = null;

      console.error("Failed to start camera/microphone:", error);

      setRoomMessage(
        error instanceof Error
          ? error.message
          : "Failed to start camera and microphone",
      );
    }
  }

  return (
    <div>
      <h1>My Video Meeting</h1>

      <p>WebSocket status: {status}</p>

      <button
        onClick={() => void pingServer()}
        disabled={status !== "Connected"}
      >
        Ping server using RPC
      </button>

      <p>Server response: {serverMessage}</p>

      <hr />

      <h2>Meeting Room</h2>

      <button
        onClick={() => void createRoom()}
        disabled={status !== "Connected"}
      >
        Create room
      </button>

      <div>
        <input
          value={roomId}
          onChange={(event) => setRoomId(event.target.value)}
          placeholder="Enter room ID"
        />

        <button
          onClick={() => void joinRoom()}
          disabled={status !== "Connected" || !roomId.trim()}
        >
          Join room
        </button>
      </div>

      <p>Room ID: {roomId || "No room yet"}</p>
      <p>{roomMessage}</p>

      <hr />

      <button
        onClick={() => void setupMediaTransport()}
        disabled={status !== "Connected" || sendTransportRef.current !== null}
      >
        Setup Media Transport
      </button>

      <hr />

      <h2>Camera and Microphone</h2>

      <button
        onClick={() => void startCameraAndMic()}
        disabled={status !== "Connected"}
      >
        Start Camera and Mic
      </button>

      <div
        style={{
          width: "480px",
          height: "270px",
          background: "red",
          position: "relative",
          zIndex: 9999,
        }}
      >
        <video
          ref={localVideoRef}
          autoPlay
          muted
          playsInline
          controls
          style={{
            display: "block",
            width: "100%",
            height: "100%",
            objectFit: "cover",
            background: "transparent",
            position: "relative",
            zIndex: 10000,
          }}
        />
      </div>

      {/* <div>
        <video
          ref={localVideoRef}
          autoPlay
          muted
          playsInline
          onLoadedMetadata={(event) => {
            const video = event.currentTarget;

            console.log("Video metadata loaded (JSX):", {
              width: video.videoWidth,
              height: video.videoHeight,
              readyState: video.readyState,
            });

            void video.play().catch((error: unknown) => {
              console.error("Video playback failed (JSX):", error);
            });
          }}
          style={{
            width: "480px",
            maxWidth: "100%",
            aspectRatio: "16 / 9",
            objectFit: "cover",
            backgroundColor: "#111",
          }}
        />
      </div> */}
    </div>
  );
};

export default Vr;
