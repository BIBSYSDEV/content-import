package no.unit.contents;

import com.mysql.jdbc.StringUtils;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class ContentsUtil {


    private static final String CONTENTS_API_URL = "https://api.bibs.aws.unit.no/contents";
    public static final String ALREADY_PROCESSED_ISBN = "Already processed isbn ";
    public static final String THAT_DID_NOT_GO_WELL = "That did not go well: ";
    public static final String NUMBER_OF_ISBN_SEND = "Number of isbn send: ";
    public static final String NUMBER_OF_SUCCESSFUL_ISBN = "Number of successful isbn: ";
    public static final String INSUFFICIENT_DATA_ON_CONTENTS = "ContentsDocument does not have sufficient metadata "
        + "and has thus been ignored: ";
    public static final String SENDING = "SENDING...";
    public static final String RESPONSE = "RESPONSE: ";
    public static final String FAILED_TO_APPEND_TO_FILE = "Failed to append to file ";
    public static final String LAST_PROCESSED_ID_WAS = "last processed id was: ";
    public static final String ISBNS_TO_PROCESS = "We have %d ISBNs to process.%n";
    public static final String DONE_WITH_ISBNS = "Done with %d isbns%n";

    protected static void appendToFailedIsbnFile(String failedRow, File file) {
        try {
            FileUtils.writeStringToFile(file, failedRow, StandardCharsets.UTF_8, true);
        } catch (IOException e) {
            System.out.println(FAILED_TO_APPEND_TO_FILE + e.getMessage());
            e.printStackTrace();
        }
    }

    protected static void appendToFinishedIsbnFile(String finishedISBN, File file) {
        try {
            FileUtils.writeStringToFile(file, finishedISBN, StandardCharsets.UTF_8, true);
        } catch (IOException e) {
            System.out.println(FAILED_TO_APPEND_TO_FILE + e.getMessage());
            e.printStackTrace();
        }
    }

    protected static boolean checkValidity(ContentsDocument contents) {
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


    protected static String sendContents(String payload) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPost httppost = new HttpPost(CONTENTS_API_URL);

        StringEntity entity = new StringEntity(payload, StandardCharsets.UTF_8);
        entity.setContentType(ContentType.APPLICATION_JSON.getMimeType());

        httppost.setEntity(entity);
        httppost.setHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8");

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

}
