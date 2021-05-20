package no.unit.contents;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

public class ContentsFileImporter {

    private static final File failed_file = new File("failedIsbn_fromFile.csv");
    private static final File finished_isbn_file = new File("processedIsbns_fromFile.txt");
    public static final String TEMP_NIELSEN_IMAGES_URL = "https://utvikle.oria.no/Nielsen/images/";
    public static final String JPG_FILE_EXTENSION = ".jpg";
    public static final String NIELSEN = "NIELSEN";
    private static SortedSet<String> finishedISBNs = new TreeSet<>();
    private final ObjectMapper mapper = new ObjectMapper();
    private final NielsenFileReader nielsenFileReader;
    //TODO: remove breaks for testing
    private static int maxNumberOfUpdates = -1;
    private int counter = 0;

    public ContentsFileImporter() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL).writerWithDefaultPrettyPrinter();
        nielsenFileReader = new NielsenFileReader();
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Started " + Instant.now());
        ContentsFileImporter translocater = new ContentsFileImporter();
        if (!failed_file.exists()) {
            failed_file.createNewFile();
        }
        if (!finished_isbn_file.exists()) {
            finished_isbn_file.createNewFile();
        }
        finishedISBNs.addAll(FileUtils.readLines(finished_isbn_file, StandardCharsets.UTF_8));
        if (args != null && args.length > 0) {
            maxNumberOfUpdates = Integer.parseInt(args[0]);
        }
        translocater.transfer();
        System.out.println("Finished " + Instant.now());
    }

    private void transfer() throws JsonProcessingException {
        String lastIsbn = "0";
        if (!finishedISBNs.isEmpty()) {
            lastIsbn = finishedISBNs.last();
        }
        System.out.println(ContentsUtil.LAST_PROCESSED_ID_WAS + lastIsbn);
        nielsenFileReader.loadData();
        Set<String> isbnSet = nielsenFileReader.getIsbnRecordSet();
        System.out.printf("we found %d metadataRecords %n", isbnSet.size());
        Set<String> isbnImageSet = nielsenFileReader.getIsbnImageSet();
        System.out.printf("we found %d images %n", isbnImageSet.size());
        isbnSet.addAll(isbnImageSet);
        System.out.printf(ContentsUtil.ISBNS_TO_PROCESS, isbnSet.size());

        List<File> fileList = nielsenFileReader.getFileList();
        for (File file : fileList) {
            List<Record> recordList = nielsenFileReader.readFile(file);
            for (Record record : recordList) {
                this.exportIsbnToDynamoDBFullContentsDocument(record);
                if (counter >= maxNumberOfUpdates && maxNumberOfUpdates != -1) {
                    System.exit(0);
                }
                isbnSet.remove(record.getIsbn13());
            }
        }
        System.out.printf("Still %d to process (image links only)", isbnSet.size());
        for (String isbn : isbnSet) {
            this.exportIsbnToDynamoDBImageOnly(isbn);
        }
        System.out.println(ContentsUtil.NUMBER_OF_ISBN_SEND + finishedISBNs.size());
    }

    private void exportIsbnToDynamoDBFullContentsDocument(Record record) throws JsonProcessingException {
        String isbn = record.getIsbn13();
        System.out.printf(ContentsUtil.DONE_WITH_ISBNS, finishedISBNs.size());
        if (finishedISBNs.contains(isbn)) {
            System.out.println(ContentsUtil.ALREADY_PROCESSED_ISBN + isbn);
        } else {
            ContentsDocument contentsDocument = this.createContentsDocument(record);
            this.exportToDynamoDB(isbn, contentsDocument);
        }
    }

    private void exportIsbnToDynamoDBImageOnly(String isbn) throws JsonProcessingException {
        System.out.printf(ContentsUtil.DONE_WITH_ISBNS, finishedISBNs.size());
        if (finishedISBNs.contains(isbn)) {
            System.out.println(ContentsUtil.ALREADY_PROCESSED_ISBN + isbn);
        } else {
            ContentsDocument contentsDocument = this.createContentsDocumentImageOnly(isbn);
            this.exportToDynamoDB(isbn, contentsDocument);
        }
    }

    private ContentsDocument createContentsDocument(Record record) {
        ContentsDocument contentsDocument = new ContentsDocument(record.getIsbn13());
        contentsDocument.descriptionShort = record.getDescriptionBrief();
        contentsDocument.descriptionLong = record.getDescriptionFull();
        contentsDocument.tableOfContents = record.getTableOfContents();
        contentsDocument.title = record.getTitle();
        contentsDocument.dateOfPublication = record.getYear();
        if (nielsenFileReader.getIsbnImageSet().contains(record.getIsbn13())) {
            contentsDocument.imageLarge = TEMP_NIELSEN_IMAGES_URL + record.getIsbn13() + JPG_FILE_EXTENSION;
        }
        contentsDocument.created = Instant.now().toString();
        contentsDocument.source = NIELSEN;
        return contentsDocument;
    }

    private ContentsDocument createContentsDocumentImageOnly(String isbn) {
        ContentsDocument contentsDocument = new ContentsDocument(isbn);
        if (nielsenFileReader.getIsbnImageSet().contains(isbn)) {
            contentsDocument.imageLarge = TEMP_NIELSEN_IMAGES_URL + isbn + JPG_FILE_EXTENSION;
        }
        contentsDocument.created = Instant.now().toString();
        contentsDocument.source = NIELSEN;
        return contentsDocument;
    }

    private void exportToDynamoDB(String isbn, ContentsDocument contentsDocument) throws JsonProcessingException {
        boolean isValidContentsDocument = ContentsUtil.checkValidity(contentsDocument);
        if (isValidContentsDocument) {
            final ContentsPayload contentsPayload = new ContentsPayload(contentsDocument);
            String payload = mapper.writeValueAsString(contentsPayload);
            try {
                System.out.println(ContentsUtil.SENDING + payload);
                String response = ContentsUtil.updateContents(payload);
                System.out.println(ContentsUtil.RESPONSE + response);
                if (!(response != null && response.contains("\"statusCode\" : 20"))) {
                    ContentsUtil.appendToFailedIsbnFile(isbn, failed_file);
                    System.err.printf("isbn %s failed!%n", isbn);
                } else {
                    counter++;
                }
            } catch (Exception e) {
                System.out.println(Instant.now());
                System.out.println(ContentsUtil.THAT_DID_NOT_GO_WELL + e.getMessage());
                ContentsUtil.appendToFailedIsbnFile(isbn, failed_file);
                e.printStackTrace();
            }
        } else {
            System.out.println(ContentsUtil.INSUFFICIENT_DATA_ON_CONTENTS + isbn);
        }
        finishedISBNs.add(isbn);
        ContentsUtil.appendToFinishedIsbnFile(isbn + System.lineSeparator(), finished_isbn_file);
    }


}
