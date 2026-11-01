import java.sql.*;
import java.util.*;

/**
 * Stock Connector/J cancel and query timeout through ProxySQL when the StarRocks FE authenticates the clients.
 * Usage: FeAuthCancel cancel|timeout
 * Environment: PROXY_HOST, PROXY_PORT, JDBC_USER, JDBC_PASSWORD, JDBC_QUERY.
 * Both cases send KILL QUERY over a new connection, which logs in through ProxySQL again.
 * Prints "cancelled MILLIS", "timeout MILLIS" or "error CODE MESSAGE", then "still-usable" when the connection works afterwards.
 */
public final class FeAuthCancel {
    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "cancel";
        Properties options = new Properties();
        options.setProperty("user", System.getenv("JDBC_USER"));
        options.setProperty("password", System.getenv("JDBC_PASSWORD"));
        options.setProperty("sslMode", "DISABLED");
        options.setProperty("connectTimeout", "10000");
        options.setProperty("socketTimeout", "120000");
        String url = "jdbc:mysql://" + System.getenv("PROXY_HOST") + ":" + System.getenv("PROXY_PORT") + "/information_schema";
        try (Connection connection = DriverManager.getConnection(url, options)) {
            Statement statement = connection.createStatement();
            if (mode.equals("timeout")) statement.setQueryTimeout(2);
            long start = System.nanoTime();
            if (mode.equals("cancel")) {
                new Thread(() -> {
                    try { Thread.sleep(2000); statement.cancel(); } catch (Exception e) { System.out.println("error cancel " + e); }
                }).start();
            }
            try {
                statement.executeQuery(System.getenv("JDBC_QUERY")).close();
                System.out.println("error 0 the query was not interrupted");
            } catch (SQLException e) {
                long millis = (System.nanoTime() - start) / 1000000;
                boolean wanted = mode.equals("cancel") ? e instanceof java.sql.SQLTimeoutException || e.getClass().getSimpleName().contains("Cancel")
                                                       : e instanceof java.sql.SQLTimeoutException;
                System.out.println((wanted ? (mode.equals("cancel") ? "cancelled " : "timeout ") + millis : "error " + e.getErrorCode() + " " + e.getClass().getSimpleName() + " " + e.getMessage()));
            }
            try (ResultSet rows = connection.createStatement().executeQuery("SELECT 7")) {
                if (rows.next() && rows.getInt(1) == 7) System.out.println("still-usable");
            } catch (SQLException e) {
                System.out.println("error " + e.getErrorCode() + " the connection is not usable: " + e.getMessage());
            }
        }
    }
}
