package Postgres_System;

import java.util.*;
import java.sql.SQLException;

public class Merge {

    public static ArrayList<String[]> mergeOplog(String database1, String database2) throws SQLException {
        ArrayList<String[]> Oplog1 = new ArrayList<>();
        ArrayList<String[]> Oplog2 = new ArrayList<>();
        if (Objects.equals(database1, "SQL")){
            Oplog1 = Oplog_PostgresDAO.readOplogEntries();
        }
        if (Objects.equals(database2, "SQL2")){
            Oplog2 = Oplog_PostgresDAO2.readOplogEntries();
        }
        if (Objects.equals(database1, "SQL2")){
            Oplog1 = Oplog_PostgresDAO2.readOplogEntries();
        }
        if (Objects.equals(database2, "SQL")){
            Oplog2 = Oplog_PostgresDAO.readOplogEntries();
        }

        //Postgres_System.Merge Starts
        ArrayList<String[]> dummyOplog = new ArrayList<>();
        dummyOplog.addAll(Oplog1);
        dummyOplog.addAll(Oplog2);
        dummyOplog.sort(Comparator.comparing(entry -> entry[1]));
        ArrayList<String[]> mergedOplog = new ArrayList<>();
        Set<String> processedStudentIds = new HashSet<>();

        for (int i = dummyOplog.size() - 1; i >= 0; i--) {
            String[] entry = dummyOplog.get(i);
            String studentId = entry[2];

            if (!processedStudentIds.contains(studentId)) {
                processedStudentIds.add(studentId);
                mergedOplog.add(entry);
            }
        }

        return mergedOplog;
    }


    public static void Merge(String db1, ArrayList<String[]> mergeOplog){
        for (int i=0;i<mergeOplog.size();i++){
            String[] entry = mergeOplog.get(i);
            String studentId = entry[2];
            String targetVal = entry[3];
            String database = entry[4];
            if (Objects.equals(database, "SQL")){
                try {
                    int id = PostgresDAO.getIdByStudentID(studentId);
                    if (Objects.equals(db1,"SQL")){
                        PostgresDAO.updateObtainedMarksById(id, targetVal);
                    }
                    if (Objects.equals(db1,"SQL2")){
                        PostgresDAO2.updateObtainedMarksById(id, targetVal);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (Objects.equals(database, "SQL2")){
                try {
                    int id = PostgresDAO2.getIdByStudentID(studentId);
                    if (Objects.equals(db1,"SQL")){
                        PostgresDAO.updateObtainedMarksById(id, targetVal);
                    }
                    if (Objects.equals(db1,"SQL2")){
                        PostgresDAO2.updateObtainedMarksById(id, targetVal);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void printOplog (ArrayList<String[]> oplog) {
        for (String[] entry : oplog) {
            System.out.println("ID: " + entry[0] + ", Timestamp: " + entry[1]
                    + ", Student ID: " + entry[2] + ", Target Value: " + entry[3]);
        }
    }

    public static void mergePipeline(String db1, String db2){
        try {
            ArrayList<String[]> mergedOplog = mergeOplog(db1, db2);
            System.out.println("Final Merged Oplog:");
            printOplog(mergedOplog);
            Merge(db1, mergedOplog);
            System.out.println("Merged Oplog into " + db1 + " successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws SQLException {
        String db1 = "SQL2";
        String db2 = "SQL";
        mergePipeline(db1,db2);
    }
}
