// File: src/Main.java
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        String filePath = "src/main/java/testcase.in";
        Pattern sqlPattern = Pattern.compile("^(SQL2|SQL)\\.(READ|INSERT)\\(([^)]*)\\)$");
        Pattern mergePattern = Pattern.compile("^MERGE\\.\\(([^)]+)\\)$");

        try {
            List<String> lines = Files.readAllLines(Paths.get(filePath));
            for (String line : lines) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                Matcher mergeMatcher = mergePattern.matcher(line);
                if (mergeMatcher.matches()) {
                    String params = mergeMatcher.group(1).trim();
                    String[] parts = params.split(",");
                    if (parts.length < 2) {
                        System.out.println("Invalid MERGE command format: " + line);
                        continue;
                    }
                    String db1 = parts[0].trim();
                    String db2 = parts[1].trim();
                    System.out.println("Processing MERGE command for databases: " + db1 + " and " + db2);
                    Merge.mergePipeline(db1, db2);
                    continue;
                }

                Matcher matcher = sqlPattern.matcher(line);
                if (matcher.matches()) {
                    String dbPrefix = matcher.group(1);
                    String command = matcher.group(2);
                    String params = matcher.group(3).trim();
                    if ("READ".equalsIgnoreCase(command)) {
                        // Expect one parameter: the student ID
                        String studentID = params;
                        System.out.println("Processing READ command for studentID: " + studentID);
                        try {
                            int id = "SQL".equalsIgnoreCase(dbPrefix)
                                    ? PostgresDAO.getIdByStudentID(studentID)
                                    : PostgresDAO2.getIdByStudentID(studentID);
                            String marks = "SQL".equalsIgnoreCase(dbPrefix)
                                    ? PostgresDAO.getObtainedMarksById(id)
                                    : PostgresDAO2.getObtainedMarksById(id);
                            System.out.println("Student: " + studentID + ", DB ID: " + id + ", Obtained Marks/Grade: " + marks);
                        } catch (SQLException e) {
                            System.out.println("Error processing READ for studentID: " + studentID);
                            e.printStackTrace();
                        }
                    } else if ("INSERT".equalsIgnoreCase(command)) {
                        // Expect two parameters separated by comma: studentID and new value
                        String[] parts = params.split(",");
                        if (parts.length < 2) {
                            System.out.println("Invalid INSERT command format: " + line);
                            continue;
                        }
                        String studentID = parts[0].trim();
                        String newValue = parts[1].trim();
                        try {
                            int id = "SQL".equalsIgnoreCase(dbPrefix)
                                    ? PostgresDAO.getIdByStudentID(studentID)
                                    : PostgresDAO2.getIdByStudentID(studentID);
                            String currentValue = "SQL".equalsIgnoreCase(dbPrefix)
                                    ? PostgresDAO.getObtainedMarksById(id)
                                    : PostgresDAO2.getObtainedMarksById(id);
                            System.out.println("Current value for student " + studentID + " is: " + currentValue);
                            System.out.println("Processing INSERT command for studentID: " + studentID
                                    + " with new value: " + newValue + " DB ID: " + id);
                            if ("SQL".equalsIgnoreCase(dbPrefix)) {
                                PostgresDAO.updateObtainedMarksById(id, newValue);
                                Oplog_PostgresDAO.insertOplogEntry(id, newValue);
                            } else {
                                PostgresDAO2.updateObtainedMarksById(id, newValue);
                                Oplog_PostgresDAO2.insertOplogEntry(id, newValue);
                            }
                            System.out.println("Updated student " + studentID + " with new value: " + newValue);
                        } catch (SQLException e) {
                            System.out.println("Error processing INSERT for studentID: " + studentID);
                            e.printStackTrace();
                        }
                    }
                } else {
                    System.out.println("Unrecognized command format: " + line);
                }
            }
        } catch (IOException e) {
            System.out.println("Error reading file: " + filePath);
            e.printStackTrace();
        }
    }
}