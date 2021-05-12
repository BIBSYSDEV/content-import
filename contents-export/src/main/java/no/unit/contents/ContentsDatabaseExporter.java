package no.unit.contents;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.jdbc.StringUtils;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import jersey.repackaged.com.google.common.collect.Iterables;
import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;

import javax.ws.rs.HttpMethod;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ContentsDatabaseExporter {

    private static final String IMAGES_BASE_URL = "https://contents.bibsys.no/content/images/";

    private static final String SELECT_ISBN_STATEMENT =
            "SELECT book_id, value AS isbn, id FROM identificator WHERE id > ? ORDER BY id";
    private static final String SELECT_BOOK_STATEMENT =
            "SELECT id AS book_id, title, year FROM book WHERE id = ?";
    private static final String SELECT_DESCRIPTION_STATEMENT =
            "SELECT book_id, type, text, source FROM description WHERE book_id = ?";
    private static final String SELECT_IMAGE_STATEMENT =
            "SELECT book_id, path, type, source FROM image WHERE book_id = ?";

    public static final String COLUMN_ID = "id";
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
    public static final String FOR_BOOK_ID = " for book_id ";
    public static final String SLASH = "/";
    public static final String ESCAPED_DOT = "\\.";

    private static final File failed_file = new File("failedIsbn.csv");
    private static final File crashed_file = new File("crashedIsbn.txt");
    private static final File finished_isbn_file = new File("processedIsbns.txt");
    private static final File finished_id_file = new File("processedIds.txt");
    private static final File insufficient_isbn_file = new File("insufficient.txt");
    private static CopyOnWriteArrayList<String> finishedISBNs = new CopyOnWriteArrayList<>();
    private static String lastId = "0";

    private final ObjectMapper mapper = new ObjectMapper();
    private final MySQLConnection mysql = new MySQLConnection();

    public ContentsDatabaseExporter() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL).writerWithDefaultPrettyPrinter();
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        System.out.println("Started " + Instant.now());
        ContentsDatabaseExporter exporter = new ContentsDatabaseExporter();
        if (!failed_file.exists()) {
            failed_file.createNewFile();
        }
        if (!crashed_file.exists()) {
            crashed_file.createNewFile();
        }
        if (!insufficient_isbn_file.exists()) {
            insufficient_isbn_file.createNewFile();
        }
        if (!finished_id_file.exists()) {
            finished_id_file.createNewFile();
        }
        List<String> idList = FileUtils.readLines(finished_id_file, StandardCharsets.UTF_8);
        lastId = String.valueOf(idList.stream()
                .mapToInt(Integer::parseInt)
                .max()
                .orElse(0));
        System.out.println(ContentsUtil.LAST_PROCESSED_ID_WAS + ContentsDatabaseExporter.lastId);
        if (!finished_isbn_file.exists()) {
            finished_isbn_file.createNewFile();
        }
        finishedISBNs.addAll(FileUtils.readLines(finished_isbn_file, StandardCharsets.UTF_8));
        if (args.length > 0) {
            List<Identificators> isbnBookIdList = exporter.readFromFailedFile();
            exporter.export(isbnBookIdList);
        } else {
            exporter.export();
        }
        System.out.println("Finished " + Instant.now());
    }

    private List<Identificators> readFromFailedFile() throws IOException {
        List<Identificators> identificatorsList = new CopyOnWriteArrayList<>();
        List<String> lines = FileUtils.readLines(failed_file, StandardCharsets.UTF_8);
        for (String line : lines) {
            String[] split = line.split(COMMA);
            identificatorsList.add(new Identificators(split[0], split[1], split[2]));
        }
        return identificatorsList;
    }

    private void export(List<Identificators> isbnBookIdList)
            throws SQLException, JsonProcessingException, ClassNotFoundException {
        for (Identificators identificators : isbnBookIdList) {
            this.exportIsbnToDynamoDB(identificators);
        }
        System.out.println(ContentsUtil.NUMBER_OF_ISBN_SEND + finishedISBNs.size());
    }

    private void export() throws SQLException, ClassNotFoundException {
        List<Identificators> isbnBookIdList = new ArrayList<>();
        try (Connection connection = mysql.getConnection()) {
            PreparedStatement isbnStatement = connection.prepareStatement(SELECT_ISBN_STATEMENT);
            isbnStatement.setString(1, lastId);
            ResultSet isbnResultSet = isbnStatement.executeQuery();
            while (isbnResultSet.next()) {
                String id = isbnResultSet.getString(COLUMN_ID);
                String isbn = isbnResultSet.getString(COLUMN_ISBN);
                String bookId = isbnResultSet.getString(COLUMN_BOOK_ID);
                isbnBookIdList.add(new Identificators(id, isbn, bookId));
            }
        }
        System.out.printf(ContentsUtil.ISBNS_TO_PROCESS, isbnBookIdList.size());
        //split list in 20 sublist and run them in parallel
        final Iterable<List<Identificators>> partition = Iterables.partition(isbnBookIdList, isbnBookIdList.size() / 20);
        partition.forEach(part -> {
            Stream<Identificators> stream = part.parallelStream();
            stream.forEach(identificators -> {
                try {
                    this.exportIsbnToDynamoDB(identificators);
                } catch (SQLException | JsonProcessingException | ClassNotFoundException throwables) {
                    throwables.printStackTrace();
                }
            });
        });
        System.out.println(ContentsUtil.NUMBER_OF_ISBN_SEND + finishedISBNs.size());
    }

    private void exportIsbnToDynamoDB(Identificators identificators)
            throws SQLException, JsonProcessingException, ClassNotFoundException {

        System.out.printf(ContentsUtil.DONE_WITH_ISBNS, finishedISBNs.size());
        try (Connection connection = new MySQLConnection().getConnection()) {
            PreparedStatement bookStatement = connection.prepareStatement(SELECT_BOOK_STATEMENT);
            PreparedStatement descriptionStatement = connection.prepareStatement(SELECT_DESCRIPTION_STATEMENT);
            PreparedStatement imageStatement = connection.prepareStatement(SELECT_IMAGE_STATEMENT);
            if (finishedISBNs.contains(identificators.isbn)) {
                System.out.println(
                        ContentsUtil.ALREADY_PROCESSED_ISBN + identificators.isbn + FOR_BOOK_ID
                                + identificators.bookId);
            } else {
                ContentsDocument contentsDocument = this.createContentsDocument(identificators.isbn);
                bookStatement.setString(1, identificators.bookId);
                this.findBookMetadata(bookStatement, contentsDocument);
                descriptionStatement.setString(1, identificators.bookId);
                this.findDescriptionData(descriptionStatement, contentsDocument);
                imageStatement.setString(1, identificators.bookId);
                this.findImagePath(imageStatement, contentsDocument);
                boolean isValidContentsDocument = ContentsUtil.checkValidity(contentsDocument);
                if (isValidContentsDocument) {
                    final ContentsPayload contentsPayload = new ContentsPayload(contentsDocument);
                    String payload = mapper.writeValueAsString(contentsPayload);
                    try {
                        System.out.println(ContentsUtil.SENDING + payload);
                        String response = ContentsUtil.updateContents(payload);
                        System.out.println(ContentsUtil.RESPONSE + response);
                        if (!response.contains("\"statusCode\" : 201")) {
                            ContentsUtil.appendToFailedIsbnFile(
                                    identificators.id + COMMA + identificators.isbn + COMMA + identificators.bookId + System.lineSeparator(),
                                    failed_file);
                            System.err.printf("isbn %s failed!", identificators.isbn);
                        }
                    } catch (Exception e) {
                        System.err.println(Instant.now());
                        System.err.println(ContentsUtil.THAT_DID_NOT_GO_WELL + e.getMessage());
                        ContentsUtil.appendToFailedIsbnFile(
                                identificators.id + COMMA + identificators.isbn + COMMA + identificators.bookId + System.lineSeparator(),
                                crashed_file);
                        e.printStackTrace();
                    }
                } else {
                    System.err.println(ContentsUtil.INSUFFICIENT_DATA_ON_CONTENTS + identificators.isbn);
                    ContentsUtil.appendToInsufficientIsbnFile(identificators.isbn + System.lineSeparator(),
                            insufficient_isbn_file);
                }
                finishedISBNs.add(identificators.isbn);
                ContentsUtil.appendToFinishedIsbnFile(identificators.isbn + System.lineSeparator(),
                        finished_isbn_file);
                this.appendToFinishedIDFile(identificators.id + System.lineSeparator());
            }
        }
    }

    private void appendToFinishedIDFile(String finishedID) {
        try {
            FileUtils.writeStringToFile(finished_id_file, finishedID, StandardCharsets.UTF_8, true);
        } catch (IOException e) {
            System.err.println(ContentsUtil.FAILED_TO_APPEND_TO_FILE + e.getMessage());
            e.printStackTrace();
        }
    }

    private void findImagePath(PreparedStatement statement, ContentsDocument contentsDocument) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                String type = resultSet.getString(COLUMN_TYPE);
                String path = preventNullString(resultSet.getString(COLUMN_PATH));
                path = this.dealWithOldBIBSYSpath(path);
                path = IMAGES_BASE_URL + path;
                if (this.isImagePresent(path)) {
                    switch (type) {
                        case SMALL_IMAGE_TYPE:
                            contentsDocument.imageSmall = path;
                            break;
                        case LARGE_IMAGE_TYPE:
                            contentsDocument.imageLarge = path;
                            break;
                        case ORIGINAL_IMAGE_TYPE:
                            contentsDocument.imageOriginal = path;
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
            if (StringUtils.isEmptyOrWhitespaceOnly(contentsDocument.source)) {
                try {
                    final String source = resultSet.getString(COLUMN_SOURCE);
                    contentsDocument.source = preventNullString(source);
                } catch (SQLException ex) {
                    contentsDocument.source = "BIBSYS";
                }
            }
        }
    }

    private boolean isImagePresent(String urlpath) {
        boolean imageIsPresent = false;
        try {
            URL url = new URL(urlpath);
            HttpURLConnection.setFollowRedirects(true);
            HttpURLConnection huc = (HttpURLConnection) url.openConnection();
            huc.setRequestMethod(HttpMethod.HEAD);
            int responseCode = huc.getResponseCode();
            imageIsPresent = responseCode < 300;
        } catch (IOException e) {
            System.err.println("Image was not found: " + urlpath);
        }
        return imageIsPresent;
    }

    public String dealWithOldBIBSYSpath(String urlpath) {
        //short BIBSYS path
        Pattern pattern = Pattern.compile("^\\w*\\/\\w*\\.\\w*$");
        final Matcher matcher = pattern.matcher(urlpath);
        if (matcher.matches()) {
            final String[] split = urlpath.split(ESCAPED_DOT);
            final char[] chars = split[0].toCharArray();
            if (chars.length > 2) {
                final char lastDigit = chars[chars.length - 1];
                final char secondLastDigit = chars[chars.length - 2];
                final String[] urlSplit = urlpath.split(SLASH);
                StringBuilder str = new StringBuilder();
                str.append(urlSplit[0])
                        .append(SLASH)
                        .append(lastDigit)
                        .append(SLASH)
                        .append(secondLastDigit)
                        .append(SLASH)
                        .append(urlSplit[1]);
                urlpath = str.toString();
            }
        }
        return urlpath;
    }

    public void findDescriptionData(PreparedStatement statement, ContentsDocument contentsDocument)
            throws SQLException {
        try (ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                String type = resultSet.getString(COLUMN_TYPE);
                String text = resultSet.getString(COLUMN_TEXT);
                text = preventNullString(text);
                text = Jsoup.clean(text, Whitelist.relaxed());
                String descLong = EMPTY_STRING;
                switch (type) {
                    case AUTHOR_TYPE:
                        contentsDocument.author = text;
                        break;
                    case CONTENTS_TYPE:
                        contentsDocument.tableOfContents = text;
                        break;
                    case SUMMARY_TYPE:
                        contentsDocument.summary = text;
                        break;
                    case REVIEW_TYPE:
                        contentsDocument.review = text;
                        break;
                    case PROMOTIONAL_TYPE:
                        contentsDocument.promotional = text;
                        break;
                    case DESCRIPTION_SHORT_TYPE:
                        contentsDocument.descriptionShort = text;
                        break;
                    case DESCRIPTION_LONG_TYPE:
                        descLong = text;
                        break;
                    default:
                        if (!text.isEmpty()) {
                            System.out.println(UNKNOWN_METADATA_TYPE + type);
                            System.out.println(WITH_VALUE);
                            System.out.println(text);
                        }
                        break;
                }
                // do not add the long description field if it has the same text as the short description field
                if (!descLong.equals(contentsDocument.descriptionShort) && !StringUtils.isEmptyOrWhitespaceOnly(
                        descLong)) {
                    contentsDocument.descriptionLong = descLong;
                }
                contentsDocument.source = preventNullString(resultSet.getString(COLUMN_SOURCE));
            }
        }
    }

    private void findBookMetadata(PreparedStatement statement, ContentsDocument contentsDocument) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                contentsDocument.title = preventNullString(resultSet.getString(COLUMN_TITLE));
                contentsDocument.dateOfPublication = preventNullString(resultSet.getString(COLUMN_YEAR));
            }
        }
    }

    private String preventNullString(String str) {
        return str == null ? EMPTY_STRING : str.trim();
    }

    protected ContentsDocument createContentsDocument(String isbn) {
        return new ContentsDocument(isbn);
    }
}
