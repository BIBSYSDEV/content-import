package no.unit.contents;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.jdbc.StringUtils;
import org.apache.commons.io.FileUtils;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ContentsDatabaseExporter {

    private static final String FILE_NAME = "failedIsbn.csv";
    private static final String CONTENTS_API_URL = "https://api.sandbox.bibs.aws.unit.no/contents";
    private static final String DATABASE_URI = "jdbc:mysql://mysql.bibsys.no/contents";
    private static final String USER = "contents";
    private static final String PASSWORD = "Pz48t39qTmBdUsXZ";
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String CONNECTION_PARAMS =
            String.format("%s?user=%s&password=%s", DATABASE_URI, USER, PASSWORD);

    private static final String SELECT_ISBN_STATEMENT =
            "SELECT `book_id`, `value` AS `isbn` FROM `identificator`";
    private static final String SELECT_BOOK_STATEMENT =
            "SELECT `id` AS `book_id`, `title`, `year` FROM `book` WHERE `id` = ?";
    private static final String SELECT_DESCRIPTION_STATEMENT =
            "SELECT `book_id`, `type`, `text`, `source` FROM `description` WHERE `book_id` = ?";
    private static final String SELECT_IMAGE_STATEMENT =
            "SELECT `book_id`, `path`, `type`, `source` FROM `image` WHERE `book_id` = ?";

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
    public static final String COMMA = ", ";

    public static final String UNKNOWN_METADATA_TYPE = "Unknown metadata type: ";
    public static final String WITH_VALUE = "with value: ";
    public static final String DUPLICATE_ISBN_ID = "Duplicate isbn ";
    public static final String FOR_BOOK_ID = " for book_id ";
    public static final String THAT_DID_NOT_GO_WELL = "That did not go well: ";
    public static final String NUMBER_OF_ISBN_SEND = "Number of isbn send: ";
    public static final String NUMBER_OF_SUCCESSFUL_ISBN = "Number of successful isbn: ";
    public static final String INSUFFICIENT_DATA_ON_CONTENTS = "ContentsDocument does not have sufficient metadata and has thus been ignored: ";
    public static final String SENDING = "SENDING...";
    public static final String RESPONSE = "RESPONSE: ";
    public static final String FAILED_TO_APPEND_TO_FILE = "Failed to append to file ";

    private final ObjectMapper mapper = new ObjectMapper();
    private final File file = new File(FILE_NAME);

    public ContentsDatabaseExporter() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL).writerWithDefaultPrettyPrinter();
    }


    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        System.out.println("Started " + Instant.now());
        ContentsDatabaseExporter exporter = new ContentsDatabaseExporter();
        if (!StringUtils.isNullOrEmpty(args[0])) {
            List<ValuePair> isbnBookIdList = exporter.readFromFile(args[0]);
            exporter.export(isbnBookIdList);
        } else {
            exporter.export();
        }
        System.out.println("Finished " + Instant.now());
    }

    private List<ValuePair> readFromFile(String filename) throws IOException {
        File file = new File(filename);
        List<ValuePair> valuePairList = new ArrayList<>();
        List<String> lines = FileUtils.readLines(file, StandardCharsets.UTF_8);
        for (String line : lines) {
            String[] split = line.split(COMMA);
            valuePairList.add(new ValuePair(split[0], split[1]));
        }
        return valuePairList;
    }


    private void export(List<ValuePair> isbnBookIdList) throws ClassNotFoundException, SQLException, JsonProcessingException {
        List<ContentsDocument> contentsList = new ArrayList<>();
        Set<String> finishedISBNs = new HashSet<>();
        Class.forName(JDBC_DRIVER);
        try (Connection connection = DriverManager.getConnection(CONNECTION_PARAMS)) {
            PreparedStatement bookStatement = connection.prepareStatement(SELECT_BOOK_STATEMENT);
            PreparedStatement descriptionStatement = connection.prepareStatement(SELECT_DESCRIPTION_STATEMENT);
            PreparedStatement imageStatement = connection.prepareStatement(SELECT_IMAGE_STATEMENT);
            for (ValuePair valuePair : isbnBookIdList) {
                this.exportIsbnToDynamoDB(contentsList, finishedISBNs, bookStatement, descriptionStatement,
                        imageStatement, valuePair.isbn, valuePair.bookId);
            }
        }
        System.out.println(NUMBER_OF_ISBN_SEND + finishedISBNs.size());
        System.out.println(NUMBER_OF_SUCCESSFUL_ISBN + contentsList.size());
    }

    private void export() throws ClassNotFoundException, SQLException, JsonProcessingException {
        List<ContentsDocument> contentsList = new ArrayList<>();
        Set<String> finishedISBNs = new HashSet<>();
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
                this.exportIsbnToDynamoDB(contentsList, finishedISBNs, bookStatement, descriptionStatement, imageStatement, isbn, bookId);
            }
        }
        System.out.println(NUMBER_OF_ISBN_SEND + finishedISBNs.size());
        System.out.println(NUMBER_OF_SUCCESSFUL_ISBN + contentsList.size());
    }

    private void exportIsbnToDynamoDB(List<ContentsDocument> contentsList, Set<String> finishedISBNs,
                                      PreparedStatement bookStatement, PreparedStatement descriptionStatement,
                                      PreparedStatement imageStatement, String isbn, String bookId)
            throws SQLException, JsonProcessingException {

        if (finishedISBNs.contains(isbn)) {
            System.out.println(DUPLICATE_ISBN_ID + isbn + FOR_BOOK_ID + bookId);
        } else {
            ContentsDocument contentsDocument = this.createContentsDocument(isbn);
            bookStatement.setString(1, bookId);
            this.findBookMetadata(bookStatement, contentsDocument);
            descriptionStatement.setString(1, bookId);
            this.findDescriptionData(descriptionStatement, contentsDocument);
            imageStatement.setString(1, bookId);
            this.findImagePath(imageStatement, contentsDocument);
            boolean isValidContentsDocument = this.checkValidity(contentsDocument);
            if (isValidContentsDocument) {
                String payload = mapper.writeValueAsString(new ContentsPayload(contentsDocument));
                try {
                    System.out.println(SENDING + payload);
                    String response = this.sendContents(payload);
                    System.out.println(RESPONSE + response);
                    contentsList.add(contentsDocument);
                } catch (Exception e) {
                    System.out.println(Instant.now());
                    System.out.println(THAT_DID_NOT_GO_WELL + e.getMessage());
                    e.printStackTrace();
                    this.appendToFailedIsbnFile(isbn + COMMA + bookId);
                }
            } else {
                System.out.println(INSUFFICIENT_DATA_ON_CONTENTS + isbn);
            }
            finishedISBNs.add(bookId);
        }
    }

    private void appendToFailedIsbnFile(String failedRow) {
        try {
            FileUtils.writeStringToFile(file, failedRow, StandardCharsets.UTF_8, true);
        } catch (IOException e) {
            System.out.println(FAILED_TO_APPEND_TO_FILE + e.getMessage());
            e.printStackTrace();
        }
    }

    private boolean checkValidity(ContentsDocument contents) {
        if (StringUtils.isEmptyOrWhitespaceOnly(contents.isbn)) {
            return false;
        }
        if (StringUtils.isEmptyOrWhitespaceOnly(contents.source)) {
            return false;
        }
        StringBuilder tempDesc = new StringBuilder();
        tempDesc.append(contents.descriptionShort)
                .append(contents.descriptionLong)
                .append(contents.tableOfContents)
                .append(contents.author)
                .append(contents.summary)
                .append(contents.review)
                .append(contents.promotional);
        StringBuilder tempImg = new StringBuilder();
        tempImg.append(contents.imageSmall)
                .append(contents.imageLarge)
                .append(contents.imageOriginal);
        if (StringUtils.isEmptyOrWhitespaceOnly(tempDesc.toString()) &&
                StringUtils.isEmptyOrWhitespaceOnly(tempImg.toString())) {
            return false;
        }
        return true;
    }

    private String sendContents(String payload) throws IOException {
        URL url = new URL(CONTENTS_API_URL);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod(HttpMethod.PUT);
        con.setRequestProperty(HttpHeaders.CONTENT_TYPE,
                MediaType.APPLICATION_JSON_TYPE.withCharset(StandardCharsets.UTF_8.name()).toString());
        con.setRequestProperty(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_TYPE.toString());
        con.setDoOutput(true);
        try (OutputStream os = con.getOutputStream()) {
            byte[] input = payload.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String responseLine;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            return response.toString();
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
            if (StringUtils.isEmptyOrWhitespaceOnly(contentsDocument.source)) {
                contentsDocument.source = preventNullString(resultSet.getString(COLUMN_SOURCE));
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
            if (!descLong.equals(contentsDocument.descriptionShort) && !StringUtils.isEmptyOrWhitespaceOnly(descLong)) {
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
