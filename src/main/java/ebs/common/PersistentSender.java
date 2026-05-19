package ebs.common;

import ebs.proto.EbsProto.Envelope;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reuses outbound TCP connections for frequently contacted endpoints.
 * This avoids reconnect overhead during high-volume publish/subscription flows.
 */
public final class PersistentSender {

    private static final Map<String, Socket> SOCKETS = new ConcurrentHashMap<>();

    private PersistentSender() {}

    public static void send(String host, int port, Envelope env) {
        String key = key(host, port);
        Socket socket = SOCKETS.computeIfAbsent(key, k -> createSocket(host, port));
        if (socket == null) {
            return;
        }

        synchronized (socket) {
            try {
                NetUtil.send(socket.getOutputStream(), env);
            } catch (IOException e) {
                // Connection may be stale; close and retry once with a fresh socket.
                closeEndpoint(host, port);
                Socket retry = SOCKETS.computeIfAbsent(key, k -> createSocket(host, port));
                if (retry == null) {
                    return;
                }
                synchronized (retry) {
                    try {
                        NetUtil.send(retry.getOutputStream(), env);
                    } catch (IOException retryError) {
                        closeEndpoint(host, port);
                        System.err.println("[PersistentSender] send failed -> " + host + ":" + port
                                + " (" + retryError.getMessage() + ")");
                    }
                }
            }
        }
    }

    public static void closeAll() {
        for (String endpoint : SOCKETS.keySet()) {
            String[] parts = endpoint.split(":", 2);
            if (parts.length != 2) {
                continue;
            }
            try {
                int port = Integer.parseInt(parts[1]);
                closeEndpoint(parts[0], port);
            } catch (NumberFormatException ignored) {
                // Ignore malformed key entries.
            }
        }
        SOCKETS.clear();
    }

    private static void closeEndpoint(String host, int port) {
        Socket s = SOCKETS.remove(key(host, port));
        if (s != null) {
            try {
                s.close();
            } catch (IOException ignored) {
                // Ignore close failure.
            }
        }
    }

    private static Socket createSocket(String host, int port) {
        try {
            Socket s = new Socket();
            s.setTcpNoDelay(true);
            s.setKeepAlive(true);
            s.setSoTimeout(60_000);
            s.connect(new InetSocketAddress(host, port), 3_000);
            return s;
        } catch (IOException e) {
            System.err.println("[PersistentSender] connect failed -> " + host + ":" + port
                    + " (" + e.getMessage() + ")");
            return null;
        }
    }

    private static String key(String host, int port) {
        return host + ":" + port;
    }
}