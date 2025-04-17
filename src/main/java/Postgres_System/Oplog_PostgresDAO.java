package Postgres_System;

import java.sql.*;
import java.util.ArrayList;

public class Oplog_PostgresDAO {

    private static final String URL = "jdbc:postgresql://localhost:5432/postgres_db";
    private static final String USER = "postgres";
    private static final String PASSWORD = "root";

    public static void createOplogTable() throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS postgres_oplog (" +
                "id SERIAL PRIMARY KEY, " +
                "timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                "student_id INT, " +
                "target_val TEXT" +
                ")";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSQL);
            //System.out.println("Table 'postgres_oplog' is ready.");
        }
    }

    public static void insertOplogEntry(int studentId, String targetVal) throws SQLException {
        // Ensure table exists before inserting entry
        createOplogTable();
        String insertSQL = "INSERT INTO postgres_oplog (student_id, target_val) VALUES (?, ?)";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
            pstmt.setInt(1, studentId);
            pstmt.setString(2, targetVal);
            pstmt.executeUpdate();
            System.out.println("Inserted oplog entry for student ID: " + studentId);
        }
    }

    public static ArrayList<String[]> readOplogEntries() throws SQLException {
        ArrayList<String[]> entries = new ArrayList<>();
        String selectSQL = "SELECT * FROM postgres_oplog";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(selectSQL)) {
            while (rs.next()) {
                String[] row = new String[5];
                row[0] = String.valueOf(rs.getInt("id"));
                row[1] = rs.getTimestamp("timestamp").toString();
                row[2] = String.valueOf(rs.getInt("student_id"));
                row[3] = rs.getString("target_val");
                row[4] = "SQL"; // Added to differentiate from other DBs
                entries.add(row);
            }
        }
        return entries;
    }

    public static void printOplogEntries() throws SQLException {
        ArrayList<String[]> oplogEntries = readOplogEntries();
        for (String[] row : oplogEntries) {
            System.out.println("ID: " + row[0] + ", Timestamp: " + row[1]
                    + ", Student ID: " + row[2] + ", Target Value: " + row[3]);
        }
    }

    public static void main(String[] args) {
        try {
            //insertOplogEntry(1,"D");
            //insertOplogEntry(2,"C");
            //insertOplogEntry(3,"B");
            printOplogEntries();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}