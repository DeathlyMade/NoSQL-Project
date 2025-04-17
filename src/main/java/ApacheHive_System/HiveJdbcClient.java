package ApacheHive_System;

import java.sql.*;

public class HiveJdbcClient {
    private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final String URL         = "jdbc:hive2://localhost:10000/default"; // Hive server URL
//    private static final String USER        = "hive";   // or empty if no auth
//    private static final String PASS        = "hive";   // or empty if no auth

    public static void main(String[] args) {
        try {
            // 1) Load the Hive JDBC driver
            Class.forName(DRIVER_NAME);

            // 2) Open connection
            try (Connection conn = DriverManager.getConnection(URL);
                 Statement stmt = conn.createStatement()) {

                // 3) Create a table (if not exists)
                stmt.execute("CREATE TABLE IF NOT EXISTS pokes2 (id INT, msg STRING)");

                // 4) Insert some rows
                stmt.execute("INSERT INTO pokes2 VALUES (1, 'hello'), (2, 'world')");

                // 5) Query the table
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM pokes2")) {
                    System.out.println("id\tmsg");
                    while (rs.next()) {
                        System.out.printf("%d\t%s%n",
                                rs.getInt("id"),
                                rs.getString("msg"));
                    }
                }

                // 6) (Optional) Drop the table when done
                // stmt.execute("DROP TABLE pokes");
            }
        } catch (ClassNotFoundException e) {
            System.err.println("Hive JDBC Driver not found in classpath");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Error executing Hive query");
            e.printStackTrace();
        }
    }
}
