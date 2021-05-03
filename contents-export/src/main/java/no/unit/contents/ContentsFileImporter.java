package no.unit.contents;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class ContentsFileImporter {

    private static final File failed_file = new File("failedIsbn_fromFile.csv");
    private static final File finished_isbn_file = new File("processedIsbns_fromFile.txt");
    public static final String TEMP_NIELSEN_IMAGES_URL = "https://utvikle.oria.no/Nielsen/images/";
    public static final String JPG_FILE_EXTENSION = ".jpg";
    public static final String NIELSEN = "NIELSEN";
    private static SortedSet<String> finishedISBNs = new TreeSet<>();
    private final ObjectMapper mapper = new ObjectMapper();
    private final NielsenFileReader nielsenFileReader;
    //TODO: bremove breaks for testing
    private final int HARD_STOPP = 10;

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
        translocater.transfer();
        System.out.println("Finished " + Instant.now());
    }

    private void transfer() throws JsonProcessingException {
        List<ContentsDocument> contentsList = new ArrayList<>();
        String lastIsbn = "0";
        if (!finishedISBNs.isEmpty()) {
            lastIsbn = finishedISBNs.last();
        }
        System.out.println(ContentsUtil.LAST_PROCESSED_ISBN_WAS + lastIsbn);
        nielsenFileReader.processFiles();
        Set<String> isbnSet = nielsenFileReader.getIsbnRecordsSortedSet();
        System.out.printf("we found %d metadataRecords %n", isbnSet.size());
        Set<String> isbnImagesSortedSet = nielsenFileReader.getIsbnImagesSortedSet();
        System.out.printf("we found %d images %n", isbnImagesSortedSet.size());
        isbnSet.addAll(isbnImagesSortedSet);

        System.out.printf(ContentsUtil.ISBNS_TO_PROCESS, isbnSet.size());
        for (String isbn : isbnSet) {
            this.exportIsbnToDynamoDB(contentsList, isbn);
            if (finishedISBNs.size() > HARD_STOPP) {
                break;
            }
        }
        System.out.println(ContentsUtil.NUMBER_OF_ISBN_SEND + finishedISBNs.size());
        System.out.println(ContentsUtil.NUMBER_OF_SUCCESSFUL_ISBN + contentsList.size());
    }

    private void exportIsbnToDynamoDB(List<ContentsDocument> contentsList, String isbn) throws JsonProcessingException {
        System.out.printf(ContentsUtil.DONE_WITH_ISBNS, finishedISBNs.size());
        if (finishedISBNs.contains(isbn)) {
            System.out.println(ContentsUtil.ALREADY_PROCESSED_ISBN + isbn);
        } else {
            ContentsDocument contentsDocument = this.createContentsDocument(isbn);
            boolean isValidContentsDocument = ContentsUtil.checkValidity(contentsDocument);
            if (isValidContentsDocument) {
                final ContentsPayload contentsPayload = new ContentsPayload(contentsDocument);
                String payload = mapper.writeValueAsString(contentsPayload);
                try {
                    System.out.println(ContentsUtil.SENDING + payload);
                    String response = ContentsUtil.sendContents(payload);
                    System.out.println(ContentsUtil.RESPONSE + response);
                    contentsList.add(contentsDocument);
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

    private ContentsDocument createContentsDocument(String isbn) {
        ContentsDocument contentsDocument = new ContentsDocument(isbn);
        Record record = nielsenFileReader.getRecordMap().get(isbn);
        if (record != null) {
            contentsDocument.descriptionShort = record.getDescriptionBrief();
            contentsDocument.descriptionLong = record.getDescriptionFull();
            contentsDocument.tableOfContents = record.getTableOfContents();
            contentsDocument.title = record.getTitle();
            contentsDocument.dateOfPublication = record.getYear();
        }
        if (nielsenFileReader.getIsbnImagesSortedSet().contains(isbn)) {
            contentsDocument.imageLarge = TEMP_NIELSEN_IMAGES_URL + isbn + JPG_FILE_EXTENSION;
        }
        contentsDocument.created = Instant.now().toString();
        contentsDocument.source = NIELSEN;
        return contentsDocument;
    }


}
