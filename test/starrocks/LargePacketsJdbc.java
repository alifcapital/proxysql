import java.sql.*;
import java.util.*;
import java.security.MessageDigest;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Stock Connector/J regression for a single row crossing the classic packet cap. */
public final class LargePacketsJdbc {
    private static final int SIZE = 1024 * 1024;
    private static final int COLUMNS = 20;
    private static String query() {
        StringJoiner q = new StringJoiner(", ", "SELECT ", "");
        for (int i = 0; i < COLUMNS; ++i)
            q.add("REPEAT('" + (char)('A' + i) + "', " + SIZE + ") AS c" + i);
        return q.toString();
    }
    private static String fingerprint(ResultSet rows) throws Exception {
        MessageDigest hash = MessageDigest.getInstance("SHA-256");
        if (!rows.next() || rows.getMetaData().getColumnCount() != COLUMNS)
            throw new AssertionError("Expected one row of twenty columns");
        long bytes = 0;
        for (int i = 0; i < COLUMNS; ++i) {
            byte[] data = rows.getBytes(i + 1);
            if (data == null || data.length != SIZE) throw new AssertionError("Wrong column size: " + i);
            for (byte b : data) if (b != (byte)('A' + i)) throw new AssertionError("Wrong column content: " + i);
            hash.update(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(data.length).array());
            hash.update(data);
            bytes += data.length;
        }
        if (rows.next() || bytes != 20L * SIZE) throw new AssertionError("Wrong row count or total bytes");
        StringBuilder hex = new StringBuilder();
        for (byte b : hash.digest()) hex.append(String.format("%02x", b & 255));
        return hex.toString();
    }
    private static String run(String variant) throws Exception {
        boolean prepared = variant.contains("prepared");
        boolean compressed = variant.contains("compressed");
        String prefix = variant.equals("oracle") ? "BACKEND_" : "PROXY_";
        Properties options = new Properties();
        options.setProperty("user", System.getenv(prefix + "USER"));
        options.setProperty("password", System.getenv(prefix + "PASSWORD"));
        options.setProperty("sslMode", "DISABLED");
        options.setProperty("connectTimeout", "15000");
        options.setProperty("socketTimeout", "120000");
        options.setProperty("useCompression", String.valueOf(compressed));
        options.setProperty("useServerPrepStmts", String.valueOf(prepared));
        options.setProperty("emulateUnsupportedPstmts", "false");
        options.setProperty("tcpRcvBuf", "1048576");
        String url = "jdbc:mysql://" + System.getenv(prefix + "HOST") + ":" + System.getenv(prefix + "PORT") + "/";
        try (Connection c = DriverManager.getConnection(url, options)) {
            System.out.println("# " + variant + " driver " + c.getMetaData().getDriverVersion());
            // Stock Connector/J API exposes the flags actually used in its handshake.
            Object session = c.getClass().getMethod("getSession").invoke(c);
            Object protocol = session.getClass().getMethod("getProtocol").invoke(session);
            Object server = protocol.getClass().getMethod("getServerSession").invoke(protocol);
            long flags = ((Number)server.getClass().getMethod("getClientParam").invoke(server)).longValue();
            if (((flags & 32L) != 0) != compressed)
                throw new AssertionError("CLIENT_COMPRESS negotiation differs from requested transport");
            if (prepared) {
                try (PreparedStatement s = c.prepareStatement(query())) {
                    if (!s.getClass().getName().contains("ServerPreparedStatement"))
                        throw new AssertionError("Server-side prepare was not used");
                    try (ResultSet rows = s.executeQuery()) { return fingerprint(rows); }
                }
            }
            try (Statement s = c.createStatement()) {
                s.setFetchSize(Integer.MIN_VALUE);
                try (ResultSet rows = s.executeQuery(query())) { return fingerprint(rows); }
            }
        }
    }
    public static void main(String[] args) {
        System.out.println("1..5");
        String oracle = null;
        int failures = 0;
        String[] variants = {"oracle", "plain", "compressed", "prepared", "compressed-prepared"};
        for (int i = 0; i < variants.length; ++i) {
            try {
                String digest = run(variants[i]);
                if (i == 0) oracle = digest;
                if (!digest.equals(oracle)) throw new AssertionError("Oracle digest differs");
                System.out.println("ok " + (i + 1) + " - stock Connector/J " + variants[i] + " exact 20 MiB row");
                System.out.println("# bytes=20971520 sha256=" + digest);
            } catch (Throwable error) {
                ++failures;
                System.out.println("not ok " + (i + 1) + " - stock Connector/J " + variants[i] + " exact 20 MiB row");
                System.out.println("# " + error);
                error.printStackTrace(System.err);
            }
        }
        if (failures != 0) System.exit(1);
    }
}
