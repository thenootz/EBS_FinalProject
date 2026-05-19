package ebs.common;

import ebs.proto.EbsProto.Envelope;

import java.io.*;
import java.net.Socket;

/**
 * Utility for sending/receiving protobuf Envelope messages over TCP.
 * Protocol: 4-byte big-endian length prefix + serialized Envelope bytes.
 */
public final class NetUtil {

    private NetUtil() {}

    /** Send an Envelope over an existing socket's OutputStream. */
    public static void send(OutputStream out, Envelope env) throws IOException {
        byte[] data = env.toByteArray();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(data.length);
        dos.write(data);
        dos.flush();
    }

    /** Read one Envelope from an existing socket's InputStream. Returns null on EOF. */
    public static Envelope receive(InputStream in) throws IOException {
        DataInputStream dis = new DataInputStream(in);
        int len;
        try {
            len = dis.readInt();
        } catch (EOFException e) {
            return null;
        }
        if (len <= 0 || len > 10_000_000) return null;
        byte[] data = new byte[len];
        dis.readFully(data);
        return Envelope.parseFrom(data);
    }

    /**
     * Open a new connection, send one Envelope, and close.
     * Fire-and-forget — errors are logged but not rethrown.
     */
    public static void sendOneShot(String host, int port, Envelope env) {
        try (Socket s = new Socket(host, port)) {
            s.setSoTimeout(3_000);
            send(s.getOutputStream(), env);
        } catch (IOException e) {
            System.err.println("[NetUtil] sendOneShot failed → " + host + ":" + port
                    + " (" + e.getMessage() + ")");
        }
    }
}
