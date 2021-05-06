package no.unit.contents;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySQLConnection implements AutoCloseable {

    public static final String DATABASE_URI = "jdbc:mysql://mysql.bibsys.no/contents";
    public static final String PASSWORD = "No default password";
    public static final String USER = "no default username";
    public static final String CONNECTION_PARAMS =
        String.format("%s?user=%s&password=%s&autoReconnect=true", DATABASE_URI, USER, PASSWORD);
    public static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

    public Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName(JDBC_DRIVER);
        return DriverManager.getConnection(CONNECTION_PARAMS);
    }

    @Override
    public void close() throws Exception {
        final Connection connection = this.getConnection();
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}