package Postgres_System;

import java.sql.*;

public class PostgresDAO {

    private static final String URL = "jdbc:postgresql://localhost:5432/postgres_db";
    private static final String USER = "postgres";
    private static final String PASSWORD = "root";

    // 1. Get ID from Student ID
    public static int getIdByStudentID(String studentID) throws SQLException {
//        String query = "SELECT id FROM graderosterreport WHERE \"Student ID\" = ?";
//        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
//             PreparedStatement pstmt = conn.prepareStatement(query)) {
//            pstmt.setString(1, studentID);
//            try (ResultSet rs = pstmt.executeQuery()) {
//                if (rs.next()) {
//                    return rs.getInt("id");
//                } else {
//                    throw new SQLException("Student ID not found.");
//                }
//            }
//        }
        // convert studentID to ID
        return Integer.parseInt(studentID);
    }

    // 2. Get Obtained Marks/Grade from ID
    public static String getObtainedMarksById(int id) throws SQLException {
        String query = "SELECT \"Obtained Marks/Grade\" FROM graderosterreport WHERE id = ?";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setInt(1, id);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("Obtained Marks/Grade");
                } else {
                    throw new SQLException("ID not found.");
                }
            }
        }
    }

    // 3. Update Obtained Marks/Grade by ID
    public static void updateObtainedMarksById(int id, String newValue) throws SQLException {
        String update = "UPDATE graderosterreport SET \"Obtained Marks/Grade\" = ? WHERE id = ?";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(update)) {
            pstmt.setString(1, newValue);
            pstmt.setInt(2, id);
            int rows = pstmt.executeUpdate();
            if (rows == 0) {
                throw new SQLException("Update failed. ID not found.");
            } else {
                System.out.println("Updated successfully.");
            }
        }
    }

    public static void main(String[] args) {
        try {
            Class.forName("org.postgresql.Driver");

            // Example: Get ID from Student ID
            //int id = getIdByStudentID("f2c567e727dd3730f75b90f59460f6ab50c975cb8ea0ea653403f783df0e67df");
            int id1 = getIdByStudentID("1");
            System.out.println("ID: " + id1);

            // Example: Get marks by ID
            String marks = getObtainedMarksById(id1);
            System.out.println("Obtained Marks: " + marks);

            int id2 = getIdByStudentID("2");
            System.out.println("ID: " + id2);
            // Example: Get marks by ID
            String marks2 = getObtainedMarksById(id2);
            System.out.println("Obtained Marks: " + marks2);

            // Example: Update marks
            //updateObtainedMarksById(id, "S");

            // Example: Get updated marks
            //String updatedMarks = getObtainedMarksById(id);
            //System.out.println("Updated Obtained Marks: " + updatedMarks);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
