package no.unit.contents;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ContentsDatabaseExporter {

    private static final String DATABASE_URI = "jdbc:mysql://mysql.bibsys.no/contents";
    private static final String USER = "";
    private static final String PASSWORD = "";
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String CONNECTION_PARAMS =
            String.format("%s?user=%s&password=%s", DATABASE_URI, USER, PASSWORD);
    private static final String SELECT_ISBN_STATEMENT =
            "SELECT `id` AS `book_id`, `value` AS `isbn` FROM `identificator`";
    private static final String SELECT_BOOK_STATEMENT =
            "SELECT `id` AS `book_id`, `title`, `year` FROM `book` WHERE `id` = ?";
    private static final String SELECT_DESCRIPTION_STATEMENT =
            "SELECT `book_id`, `type`, `text`, `source` FROM `description` WHERE `book_id` = ?";
    private static final String SELECT_IMAGE_STATEMENT =
            "SELECT `book_id`, `path`, `type` FROM `image` WHERE `book_id` = ?";
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
    public static final String REVIEW_TYPE = "REVIEW";
    public static final String SUMMARY_TYPE = "SUMMARY";
    public static final String PROMOTIONAL_TYPE = "PROMOTIONAL";
    public static final String SMALL_IMAGE_TYPE = "SMALL";
    public static final String LARGE_IMAGE_TYPE = "LARGE";
    public static final String ORIGINAL_IMAGE_TYPE = "ORIGINAL";
    public static final String EMPTY_STRING = "";
    public static final String UNKNOWN_METADATA_TYPE = "Unknown metadata type: ";
    public static final String WITH_VALUE = "with value: ";
    public static final String DUPLICATE_BOOK_ID = "Duplicate book_id ";
    private final ObjectMapper mapper = new ObjectMapper();

    public ContentsDatabaseExporter() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL).writerWithDefaultPrettyPrinter();
    }


    public static void main(String[] args) throws SQLException, ClassNotFoundException, JsonProcessingException {
        ContentsDatabaseExporter exporter = new ContentsDatabaseExporter();
        List<ContentsDocument> contentsList = exporter.readAllIsbn();
        System.out.println("Number of isbn i database: " + contentsList.size());
    }


    private List<ContentsDocument> readAllIsbn() throws ClassNotFoundException, SQLException, JsonProcessingException {
        List<ContentsDocument> contentsList = new ArrayList<>();
        Set<String> finishedBookIDs = new HashSet<>();
        Class.forName(JDBC_DRIVER);
        try (Connection connection = DriverManager.getConnection(CONNECTION_PARAMS)) {
            PreparedStatement isbnStatement = connection.prepareStatement(SELECT_ISBN_STATEMENT);
            PreparedStatement bookStatement = connection.prepareStatement(SELECT_BOOK_STATEMENT);
            PreparedStatement descriptionStatement = connection.prepareStatement(SELECT_DESCRIPTION_STATEMENT);
            PreparedStatement imageStatement = connection.prepareStatement(SELECT_IMAGE_STATEMENT);
            ResultSet isbnResultSet = isbnStatement.executeQuery();
            while (isbnResultSet.next()) {
                String isbn = isbnResultSet.getString(COLUMN_ISBN);
                String bookId = isbnResultSet.getString(COLUMN_BOOK_ID);
                if (finishedBookIDs.contains(bookId)) {
                    System.out.println(DUPLICATE_BOOK_ID + bookId + " for isbn " + isbn);
                } else {
                    ContentsDocument contentsDocument = this.createContentsDocument(isbn);
                    bookStatement.setString(1, bookId);
                    this.findBookMetadata(bookStatement, contentsDocument);
                    descriptionStatement.setString(1, bookId);
                    this.findDescriptionData(descriptionStatement, contentsDocument);
                    imageStatement.setString(1, bookId);
                    this.findImagePath(imageStatement, contentsDocument);
                    String payload = mapper.writeValueAsString(contentsDocument);
                    System.out.println(payload);
                    try {
                        this.postToContentsApi(payload);
                    } catch (IOException e) {
                        System.out.println("That did not went well: " + e.getMessage());
                        e.printStackTrace();
                    }
                    contentsList.add(contentsDocument);
                    finishedBookIDs.add(bookId);
                }
            }
        }
        return contentsList;
    }

    private void postToContentsApi(String contentsJson) throws IOException {
        //Todo: working but ugly
        String body = "{ \"contents\": \"" + contentsJson.replace("\"", "\\\"") + "\"}";
        URL url = new URL("https://api.sandbox.bibs.aws.unit.no/contents");
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod("PUT");
        con.setRequestProperty("Content-Type", "application/json; utf-8");
        con.setRequestProperty("Accept", "application/json");
        con.setDoOutput(true);
        try(OutputStream os = con.getOutputStream()) {
            byte[] input = body.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }
        try(BufferedReader br = new BufferedReader(
                new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String responseLine = null;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            System.out.println(response.toString());
        }

    }

    private void findImagePath(PreparedStatement statement, ContentsDocument contentsDocument) throws SQLException {
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            String type = resultSet.getString(COLUMN_TYPE);
            String path = resultSet.getString(COLUMN_PATH);
            switch (type) {
                case SMALL_IMAGE_TYPE:
                    contentsDocument.imageSmall = preventNullString(path);
                    break;
                case LARGE_IMAGE_TYPE:
                    contentsDocument.imageLarge = preventNullString(path);
                    break;
                case ORIGINAL_IMAGE_TYPE:
                    contentsDocument.imageOriginal = preventNullString(path);
                    break;
                default:
                    if (!preventNullString(path).isEmpty()) {
                        System.out.println(UNKNOWN_METADATA_TYPE + type);
                        System.out.println(WITH_VALUE);
                        System.out.println(path);
                    }
                    break;
            }
        }
    }

    private void findDescriptionData(PreparedStatement statement, ContentsDocument contentsDocument)
            throws SQLException {
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            String type = resultSet.getString(COLUMN_TYPE);
            String text = resultSet.getString(COLUMN_TEXT);
            String descLong = EMPTY_STRING;
            switch (type) {
                case AUTHOR_TYPE:
                    contentsDocument.author = preventNullString(text);
                    break;
                case CONTENTS_TYPE:
                    contentsDocument.tableOfContents = preventNullString(text);
                    break;
                case SUMMARY_TYPE:
                    contentsDocument.summary = preventNullString(text);
                    break;
                case REVIEW_TYPE:
                    contentsDocument.review = preventNullString(text);
                    break;
                case PROMOTIONAL_TYPE:
                    contentsDocument.promotional = preventNullString(text);
                    break;
                case DESCRIPTION_SHORT_TYPE:
                    contentsDocument.descriptionShort = preventNullString(text);
                    break;
                case DESCRIPTION_LONG_TYPE:
                    descLong = preventNullString(text);
                    break;
                default:
                    if (!preventNullString(text).isEmpty()) {
                        System.out.println(UNKNOWN_METADATA_TYPE + type);
                        System.out.println(WITH_VALUE);
                        System.out.println(text);
                    }
                    break;
            }
            // do not add the long description field if it has the same text as the short description field
            if (!descLong.equals(contentsDocument.descriptionShort)) {
                contentsDocument.descriptionLong = descLong;
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
