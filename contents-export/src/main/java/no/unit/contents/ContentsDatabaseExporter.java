package no.unit.contents;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.jdbc.StringUtils;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ContentsDatabaseExporter {

    private static final String FAILED_ISBN_CSV = "failedIsbn.csv";
    private static final String FINISHED_ISBN_TXT = "processedIsbns.txt";
    private static final String IMAGES_BASE_URL = "https://contents.bibsys.no/content/images/";
    private static final String CONTENTS_API_URL = "https://api.sandbox.bibs.aws.unit.no/contents";

    private static final String SELECT_ISBN_STATEMENT =
        "SELECT `book_id`, `value` AS `isbn` FROM `identificator` WHERE value > ? ORDER BY `value`";
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
    public static final String ALREADY_PROCESSED_ISBN = "Already processed isbn ";
    public static final String FOR_BOOK_ID = " for book_id ";
    public static final String THAT_DID_NOT_GO_WELL = "That did not go well: ";
    public static final String NUMBER_OF_ISBN_SEND = "Number of isbn send: ";
    public static final String NUMBER_OF_SUCCESSFUL_ISBN = "Number of successful isbn: ";
    public static final String INSUFFICIENT_DATA_ON_CONTENTS = "ContentsDocument does not have sufficient metadata "
        + "and has thus been ignored: ";
    public static final String SENDING = "SENDING...";
    public static final String RESPONSE = "RESPONSE: ";
    public static final String FAILED_TO_APPEND_TO_FILE = "Failed to append to file ";
    public static final String SLASH = "/";
    public static final String ESCAPED_DOT = "\\.";
    public static final String LAST_PROCESSED_ISBN_WAS = "last processed isbn was: ";
    public static final String ISBNS_TO_PROCESS_N = "We have %d ISBNs to process.%n";
    public static final String DONE_WITH_D_ISBNS_N = "Done with %d isbns%n";

    private final ObjectMapper mapper = new ObjectMapper();
    private static final File failed_file = new File(FAILED_ISBN_CSV);
    private static final File finished_file = new File(FINISHED_ISBN_TXT);
    private static SortedSet<String> finishedISBNs = new TreeSet<>();
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
        if (!finished_file.exists()) {
            finished_file.createNewFile();
        }
        finishedISBNs.addAll(FileUtils.readLines(finished_file, StandardCharsets.UTF_8));
        if (args.length > 0) {
            List<ValuePair> isbnBookIdList = exporter.readFromFailedFile(args[0]);
            exporter.export(isbnBookIdList);
        } else {
            exporter.export();
        }
        System.out.println("Finished " + Instant.now());
    }

    private List<ValuePair> readFromFailedFile(String filename) throws IOException {
        File file = new File(filename);
        List<ValuePair> valuePairList = new ArrayList<>();
        List<String> lines = FileUtils.readLines(file, StandardCharsets.UTF_8);
        for (String line : lines) {
            String[] split = line.split(COMMA);
            valuePairList.add(new ValuePair(split[0], split[1]));
        }
        return valuePairList;
    }

    private void export(List<ValuePair> isbnBookIdList)
        throws SQLException, JsonProcessingException, ClassNotFoundException {
        List<ContentsDocument> contentsList = new ArrayList<>();
        for (ValuePair valuePair : isbnBookIdList) {
            this.exportIsbnToDynamoDB(contentsList, valuePair);
        }
        System.out.println(NUMBER_OF_ISBN_SEND + finishedISBNs.size());
        System.out.println(NUMBER_OF_SUCCESSFUL_ISBN + contentsList.size());
    }

    private void export() throws SQLException, JsonProcessingException, ClassNotFoundException {
        List<ContentsDocument> contentsList = new ArrayList<>();
        List<ValuePair> isbnBookIdList = new ArrayList<>();
        String lastIsbn = "0";
        if (!finishedISBNs.isEmpty()) {
            lastIsbn = finishedISBNs.last();
        }
        System.out.println(LAST_PROCESSED_ISBN_WAS + lastIsbn);
        try (Connection connection = mysql.getConnection()) {
            PreparedStatement isbnStatement = connection.prepareStatement(SELECT_ISBN_STATEMENT);
            isbnStatement.setString(1, lastIsbn);
            ResultSet isbnResultSet = isbnStatement.executeQuery();
            while (isbnResultSet.next()) {
                String isbn = isbnResultSet.getString(COLUMN_ISBN);
                String bookId = isbnResultSet.getString(COLUMN_BOOK_ID);
                isbnBookIdList.add(new ValuePair(isbn, bookId));
            }
        }
        System.out.printf(ISBNS_TO_PROCESS_N, isbnBookIdList.size());
        for (ValuePair valuePair : isbnBookIdList) {
            this.exportIsbnToDynamoDB(contentsList, valuePair);
        }
        System.out.println(NUMBER_OF_ISBN_SEND + finishedISBNs.size());
        System.out.println(NUMBER_OF_SUCCESSFUL_ISBN + contentsList.size());
    }

    private void exportIsbnToDynamoDB(List<ContentsDocument> contentsList, ValuePair pair)
        throws SQLException, JsonProcessingException, ClassNotFoundException {

        System.out.printf(DONE_WITH_D_ISBNS_N, finishedISBNs.size());
        MySQLConnection mysql = new MySQLConnection();
        try (Connection connection = mysql.getConnection()) {
            PreparedStatement bookStatement = connection.prepareStatement(SELECT_BOOK_STATEMENT);
            PreparedStatement descriptionStatement = connection.prepareStatement(SELECT_DESCRIPTION_STATEMENT);
            PreparedStatement imageStatement = connection.prepareStatement(SELECT_IMAGE_STATEMENT);
            if (finishedISBNs.contains(pair.isbn)) {
                System.out.println(ALREADY_PROCESSED_ISBN + pair.isbn + FOR_BOOK_ID + pair.bookId);
            } else {
                ContentsDocument contentsDocument = this.createContentsDocument(pair.isbn);
                bookStatement.setString(1, pair.bookId);
                this.findBookMetadata(bookStatement, contentsDocument);
                descriptionStatement.setString(1, pair.bookId);
                this.findDescriptionData(descriptionStatement, contentsDocument);
                imageStatement.setString(1, pair.bookId);
                this.findImagePath(imageStatement, contentsDocument);
                boolean isValidContentsDocument = this.checkValidity(contentsDocument);
                if (isValidContentsDocument) {
                    final ContentsPayload contentsPayload = new ContentsPayload(contentsDocument);
                    String payload = mapper.writeValueAsString(contentsPayload);
                    try {
                        System.out.println(SENDING + payload);
                        String response = this.sendContents(payload);
                        System.out.println(RESPONSE + response);
                        contentsList.add(contentsDocument);
                    } catch (Exception e) {
                        System.out.println(Instant.now());
                        System.out.println(THAT_DID_NOT_GO_WELL + e.getMessage());
                        e.printStackTrace();
                        this.appendToFailedIsbnFile(pair.isbn + COMMA + pair.bookId);
                    }
                } else {
                    System.out.println(INSUFFICIENT_DATA_ON_CONTENTS + pair.isbn);
                }
                finishedISBNs.add(pair.isbn);
                this.appendToFinishedIsbnFile(pair.isbn + System.lineSeparator());
            }
        }
    }

    private void appendToFailedIsbnFile(String failedRow) {
        try {
            FileUtils.writeStringToFile(failed_file, failedRow, StandardCharsets.UTF_8, true);
        } catch (IOException e) {
            System.out.println(FAILED_TO_APPEND_TO_FILE + e.getMessage());
            e.printStackTrace();
        }
    }

    private void appendToFinishedIsbnFile(String finishedISBN) {
        try {
            FileUtils.writeStringToFile(finished_file, finishedISBN, StandardCharsets.UTF_8, true);
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
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPost httppost = new HttpPost(CONTENTS_API_URL);

        StringEntity entity = new StringEntity(payload);
        entity.setContentType(ContentType.APPLICATION_JSON.getMimeType());

        httppost.setEntity(entity);
        httppost.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_TYPE.toString());

        //Execute and get the response.
        HttpResponse response = httpclient.execute(httppost);
        HttpEntity responseEntity = response.getEntity();

        if (responseEntity != null) {
            try (BufferedReader br = new BufferedReader(
                new InputStreamReader(responseEntity.getContent(), StandardCharsets.UTF_8))) {
                StringBuilder resp = new StringBuilder();
                String responseLine;
                while ((responseLine = br.readLine()) != null) {
                    resp.append(responseLine.trim());
                }
                return resp.toString();
            }
        }
        return null;
    }

    private void findImagePath(PreparedStatement statement, ContentsDocument contentsDocument) throws SQLException {
        ResultSet resultSet = statement.executeQuery();
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
            System.out.println("Image was not found: " + urlpath);
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
        ResultSet resultSet = statement.executeQuery();
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
