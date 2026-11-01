import java.sql.*;
import java.util.*;

/**
 * Stock Connector/J login through ProxySQL when the StarRocks FE authenticates the clients.
 * Usage: FeAuthJdbc plain|tls|compressed|tls-compressed|prepared
 * Environment: PROXY_HOST, PROXY_PORT, JDBC_USER, JDBC_PASSWORD.
 * Prints "jdbc-ok" after a query, or "error CODE MESSAGE" when the login or the query fails.
 */
public final class FeAuthJdbc {
    public static void main(String[] args) throws Exception {
        String variant = args.length > 0 ? args[0] : "plain";
        boolean tls = variant.startsWith("tls");
        boolean compressed = variant.contains("compressed");
        Properties options = new Properties();
        options.setProperty("user", System.getenv("JDBC_USER"));
        options.setProperty("password", System.getenv("JDBC_PASSWORD"));
        options.setProperty("sslMode", tls ? "REQUIRED" : "DISABLED");
        options.setProperty("useCompression", Boolean.toString(compressed));
        options.setProperty("allowPublicKeyRetrieval", "true");
        options.setProperty("connectTimeout", "10000");
        options.setProperty("socketTimeout", "60000");
        String url = "jdbc:mysql://" + System.getenv("PROXY_HOST") + ":" + System.getenv("PROXY_PORT") + "/information_schema";
        try (Connection connection = DriverManager.getConnection(url, options)) {
            if (variant.equals("prepared")) {
                try (PreparedStatement statement = connection.prepareStatement("SELECT ? + 1")) {
                    statement.setInt(1, 41);
                    try (ResultSet rows = statement.executeQuery()) {
                        if (!rows.next() || rows.getInt(1) != 42) throw new AssertionError("wrong prepared result");
                    }
                }
            } else {
                try (Statement statement = connection.createStatement();
                     ResultSet rows = statement.executeQuery("SELECT REPEAT('j', 100000)")) {
                    if (!rows.next() || rows.getString(1).length() != 100000) throw new AssertionError("wrong result");
                }
            }
            System.out.println("jdbc-ok");
        } catch (SQLException error) {
            System.out.println("error " + error.getErrorCode() + " " + error.getMessage());
        }
    }
}
