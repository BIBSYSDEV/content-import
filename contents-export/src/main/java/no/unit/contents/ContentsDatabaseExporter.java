package no.unit.contents;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;

public class ContentsDatabaseExporter {

    private static final String DATABASE_URI = "jdbc:mysql://mysql.bibsys.no/contents";
    private static final String USER = "";
    private static final String PASSWORD = "";
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String CONNECTION_PARAMS = String.format("%s?user=%s&password=%s", DATABASE_URI, USER, PASSWORD);
    private static final String SELECT_ISBN_STATEMENT = "SELECT `id` AS `book_id`, `value` AS `isbn` FROM `identificator`";
    private static final String SELECT_BOOK_STATEMENT = "SELECT `id` AS `book_id`, `title`, `year` FROM `book` WHERE `id` = ?";
    private static final String SELECT_DESCRIPTION_STATEMENT = "SELECT `book_id`, `type`, `text`, `source` FROM `description` WHERE `book_id` = ?";
    private static final String SELECT_IMAGE_STATEMENT = "SELECT `book_id`, `path`, `type` FROM `image` WHERE `book_id` = ?";
    public static final String COLUMN_ISBN = "isbn";
    public static final String COLUMN_BOOK_ID = "book_id";
    public static final String COLUMN_TITLE = "title";
    public static final String COLUMN_YEAR = "year";
    public static final String COLUMN_TYPE = "type";
    public static final String COLUMN_TEXT = "text";
    public static final String COLUMN_SOURCE = "source";
    public static final String COLUMN_PATH = "path";
    public static final String AUTHOR_TYPE = "AUTHOR";
    public static final String CONTENTS_TYPE = "CONTENTS";
    public static final String DESCRIPTION_SHORT_TYPE = "DESCRIPTION_SHORT";
    public static final String DESCRIPTION_LONG_TYPE = "DESCRIPTION_LONG";
    public static final String SMALL_TYPE = "SMALL";
    public static final String LARGE_TYPE = "LARGE";
    public static final String ORIGINAL_TYPE = "ORIGINAL";
    public static final String EMPTY_STRING = "";

    //REVIEW
    //SUMMARY



    public static void main(String[] args) throws SQLException, ClassNotFoundException, JsonProcessingException {
        ContentsDatabaseExporter exporter = new ContentsDatabaseExporter();
        List<ContentsDocument> contentsList = exporter.readAllIsbn();
        System.out.println("Number of isbn i database: " + contentsList.size());
    }


    private List<ContentsDocument> readAllIsbn() throws ClassNotFoundException, SQLException, JsonProcessingException {
        List<ContentsDocument> contentsList = new ArrayList<>();
        Class.forName(JDBC_DRIVER);
        try (Connection connection = DriverManager.getConnection(CONNECTION_PARAMS)) {
            PreparedStatement isbnStatement = connection.prepareStatement(SELECT_ISBN_STATEMENT);
            PreparedStatement bookStatement = connection.prepareStatement(SELECT_BOOK_STATEMENT);
            PreparedStatement descriptionStatement = connection.prepareStatement(SELECT_DESCRIPTION_STATEMENT);
            PreparedStatement imageStatement = connection.prepareStatement(SELECT_IMAGE_STATEMENT);
            ResultSet isbnResultSet = isbnStatement.executeQuery();
            while (isbnResultSet.next()) {
                ContentsDocument contentsDocument = this.createContentsDocument(isbnResultSet.getString(COLUMN_ISBN));
                String bookId = isbnResultSet.getString(COLUMN_BOOK_ID);
                bookStatement.setString(1, bookId);
                this.findBookMetadata(bookStatement, contentsDocument);
                descriptionStatement.setString(1, bookId);
                this.findDescriptionData(descriptionStatement, contentsDocument);
                imageStatement.setString(1, bookId);
                this.findImagePath(imageStatement, contentsDocument);
                String json = this.toJsonString(contentsDocument);
                System.out.println(json);
                contentsList.add(contentsDocument);
            }
        }
        return contentsList;
    }

    private String toJsonString(ContentsDocument contentsDocument) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(contentsDocument);
        return json;
    }

    private void findImagePath(PreparedStatement statement, ContentsDocument contentsDocument) throws SQLException {
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            String type = resultSet.getString(COLUMN_TYPE);
            String path = resultSet.getString(COLUMN_PATH);
            switch (type) {
                case SMALL_TYPE:
                    contentsDocument.imageSmall = preventNullString(path);
                    break;
                case LARGE_TYPE:
                    contentsDocument.imageLarge = preventNullString(path);
                    break;
                case ORIGINAL_TYPE:
                    contentsDocument.imageOriginal = preventNullString(path);
                    break;
                default:
                    if (!preventNullString(path).isEmpty()) {
                        System.out.println("Unknown image type: " + type + System.lineSeparator() + "med value: "
                                + System.lineSeparator() + path);
                    }
                    break;
            }
        }
    }

    private void findDescriptionData(PreparedStatement statement, ContentsDocument contentsDocument) throws SQLException {
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            String type = resultSet.getString(COLUMN_TYPE);
            String text = resultSet.getString(COLUMN_TEXT);
            switch (type) {
                case AUTHOR_TYPE:
                    contentsDocument.author = preventNullString(text);
                    break;
                case CONTENTS_TYPE:
                    contentsDocument.tableOfContents = preventNullString(text);
                    break;
                case DESCRIPTION_SHORT_TYPE:
                    contentsDocument.descriptionShort = preventNullString(text);
                    break;
                case DESCRIPTION_LONG_TYPE:
                    contentsDocument.descriptionLong = preventNullString(text);
                    break;
                default:
                    if (!preventNullString(text).isEmpty()) {
                        System.out.println("Unknown metadata type: " + type + System.lineSeparator() + "med value: "
                                + System.lineSeparator() + text);
                    }
                    break;
            }
            contentsDocument.source = preventNullString(resultSet.getString(COLUMN_SOURCE));
        }
    }

    private void findBookMetadata(PreparedStatement statement, ContentsDocument contentsDocument) throws SQLException {
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            contentsDocument.title = preventNullString(resultSet.getString(COLUMN_TITLE));
            contentsDocument.dateOfPublication = preventNullString(resultSet.getString(COLUMN_YEAR));
        }
    }

    private String preventNullString(String str) {
        return str == null ? EMPTY_STRING : str.trim();
    }

    private ContentsDocument createContentsDocument(String isbn) {
        return new ContentsDocument(isbn);
    }

}
