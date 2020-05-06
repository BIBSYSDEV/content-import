package no.unit.contents;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class NielsenDatabaseUpdater {

    private static final String DATABASE_URI = "jdbc:mysql://mysql-utvikle.bibsys.no/contents";
    private static final String USER = "contents";
    private static final String PASSWORD = "contents";

    private static final String BOOK_SELECT_STATEMENT = "select * from book limit 100;";
    private static final String IMAGE_SELECT_STATEMENT = "select * from image;";
    private static final int ID = 1;

    private void updateDatabase() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        try (Connection connection =
                DriverManager
                        .getConnection(
                                String.format("%s?user=%s&password=%s", DATABASE_URI, USER, PASSWORD))) {
            {
                PreparedStatement statement = connection.prepareStatement(BOOK_SELECT_STATEMENT);
//            statement.setInt(1, ID);
                ResultSet resultSet = statement.executeQuery();
//                while (resultSet.next()) {
//                    System.out.println(resultSet.getString("title"));
//                }
            }

            {
                PreparedStatement statement = connection.prepareStatement(IMAGE_SELECT_STATEMENT);
//          statement.setInt(1, ID);
                ResultSet resultSet = statement.executeQuery();
                List<String> pathList = new ArrayList<>();
                while (resultSet.next()) {
                    pathList.add(resultSet.getString("path"));
                }

                pathList.stream().filter(
                        path -> !path.startsWith("original") && !path.startsWith("small") && !path.startsWith("large"))
                        .forEach(System.out::println);
            }

        }
    }

    public static void main(String... args) {
        try {
            new NielsenDatabaseUpdater().updateDatabase();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

}
