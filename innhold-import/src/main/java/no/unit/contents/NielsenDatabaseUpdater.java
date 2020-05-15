package no.unit.contents;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.mysql.jdbc.Statement;

public class NielsenDatabaseUpdater {

    private static final String DATABASE_URI = "jdbc:mysql://mysql.bibsys.no/contents";
    private static final String USER = "contents";
    private static final String PASSWORD = "Pz48t39qTmBdUsXZ";

    private static final String BOOK_SELECT_STATEMENT = "SELECT book_id FROM contents.identificator where type='ISBN13' and value = ?;";
    private static final String IMAGE_SELECT_STATEMENT = "select * from image;";

    private static final String CONTENTS_DIR = "e:\\innhold\\Nielsen\\data\\test";
    private static final String IMAGE_DIR = "e:\\innhold\\Nielsen\\images";
    private static final String ISBN_SELECT_STATEMENT = "SELECT value FROM contents.identificator WHERE type='ISBN13'";


    public static List<Record> readRecords() throws IOException {

        List<Record> records = new ArrayList<Record>();
        XmlMapper mapper = new XmlMapper();

        Files.list(Paths.get(CONTENTS_DIR))
        .filter(file -> file.toString().endsWith(".add"))
        .forEach(file -> {
            try {
                records.addAll(mapper.readValue(file.toFile(), NielsenBook.class).getRecord());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return records;
    }

    private List<String> readAllIsbn() throws ClassNotFoundException, SQLException {

        List<String> isbnList = new ArrayList<>();

        Class.forName("com.mysql.jdbc.Driver");
        try (Connection connection =
                DriverManager
                .getConnection(
                        String.format("%s?user=%s&password=%s", DATABASE_URI, USER, PASSWORD))) {
            {
                PreparedStatement statement = connection.prepareStatement(ISBN_SELECT_STATEMENT);
                ResultSet resultSet = statement.executeQuery();
                while (resultSet.next()) {
                    isbnList.add(resultSet.getString("value"));
                }
            }
        }
        return isbnList;
    }

    private String createBookInsertValues(List<Record> records) {

        return String.join(String.format(",%n"), records.stream()
                .map(record -> {
                    String title = record.getTitle();
                    if(title.contains("'")) {
                        title = title.replaceAll("'", "`");
                    }
                    if(title.contains("\\")) {
                        title = title.replace("\\", "\\\\");
                    }
                    return String.format("('%s', '%s', 'NIELSEN')", title, record.getYear() != null ? record.getYear() : "");
                })
                .collect(Collectors.toList()));
    }

    private String createISBNInsertValues(List<Record> records, List<Integer> keyList) {

        AtomicInteger counter = new AtomicInteger();

        return String.join(String.format(",\n"), records.stream()
                .map(record -> {
                    return String.format("(%d, 'ISBN13', '%s')", keyList.get(counter.getAndIncrement()), record.getIsbn13());
                })
                .collect(Collectors.toList()));
    }

    private String createDescriptionInsertValues(List<Record> records, List<Integer> keyList) {
        AtomicInteger counter = new AtomicInteger();

        List<String> insertValues = new ArrayList<>();
        records.forEach(record -> {
            String tableOfContents = record.getTableOfContents();
            if(tableOfContents.contains("'")) {
                tableOfContents = tableOfContents.replaceAll("'", "`");
            }
            if(tableOfContents.contains("\\")) {
                tableOfContents = tableOfContents.replace("\\", "\\\\");
            }
            String descriptionFull = record.getDescriptionFull();
            if(descriptionFull.contains("'")) {
                descriptionFull = descriptionFull.replaceAll("'", "`");
            }
            if(descriptionFull.contains("'")) {
                descriptionFull = descriptionFull.replace("\\", "\\\\");
            }
            String descriptionBrief = record.getDescriptionBrief();
            if(descriptionBrief.contains("'")) {
                descriptionBrief = descriptionBrief.replaceAll("'", "`");
            }
            if(descriptionBrief.contains("'")) {
                descriptionBrief = descriptionBrief.replace("\\", "\\\\");
            }
            if(record.getDescriptionBrief() != null && !record.getDescriptionBrief().isBlank()) {
                insertValues.add(String.format("(%d, 'DESCRIPTION_SHORT', 'NIELSEN', '%s')", keyList.get(counter.get()), descriptionBrief));
            }
            if(record.getDescriptionFull() != null && !record.getDescriptionFull().isBlank()) {
                insertValues.add(String.format("(%d, 'DESCRIPTION_LONG', 'NIELSEN', '%s')", keyList.get(counter.get()), descriptionFull));
            }
            if(record.getTableOfContents() != null && !record.getTableOfContents().isBlank()) {
                insertValues.add(String.format("(%d, 'CONTENTS', 'NIELSEN', '%s')", keyList.get(counter.get()), tableOfContents));
            }
            counter.getAndIncrement();
        });


        return insertValues.size() > 0 ? String.join(String.format(",%n"), insertValues) : "";

    }

    private Map<String, Set<String>> findMissingImages(List<Record> records) throws SQLException, IOException {
        System.out.println("scanning images...");

        Set<String> smallSet = Files.list(Paths.get("e:\\innhold\\Nielsen\\images\\existing\\small")).map(path -> path.getFileName().toString()).collect(Collectors.toSet());
        Set<String> largeSet = Files.list(Paths.get("e:\\innhold\\Nielsen\\images\\existing\\large")).map(path -> path.getFileName().toString()).collect(Collectors.toSet());
        Set<String> originalSet = Files.list(Paths.get("e:\\innhold\\Nielsen\\images\\existing\\original")).map(path -> path.getFileName().toString()).collect(Collectors.toSet());

        Map<String, Set<String>> existingImages = new HashMap<>();
        existingImages.put("small", smallSet);
        existingImages.put("large", largeSet);
        existingImages.put("original", originalSet);
        
        System.out.println("done...");
        return existingImages;
    }

    private static List<List<Record>> splitList(List<Record> notFoundRecords) {
        List<List<Record>> listOfLists = new ArrayList<>();
        List<Record> tempList = new ArrayList<>();
        notFoundRecords.forEach(record -> {
            tempList.add(record);
            if(tempList.size() == 1000) {
                listOfLists.add(new ArrayList<>(tempList));
                tempList.clear();
            }
        });
        if(tempList.size() != 0) {
            listOfLists.add(new ArrayList<>(tempList));
        }
        return listOfLists;
    }

    public static List<Record> findMissingRecords(NielsenDatabaseUpdater updater) throws IOException {
        List<Record> records = readRecords();

        System.out.println("#records = " + records.size());

        final Set<String> isbnList = new HashSet<>();
        try {
            isbnList.addAll(updater.readAllIsbn());
            System.out.println("#isbn = " + isbnList.size());
        } catch (ClassNotFoundException | SQLException e1) {
            e1.printStackTrace();
        }

        AtomicInteger counter = new AtomicInteger();

        List<Record> notFoundRecords = records.stream().filter(record -> {
            if(counter.incrementAndGet() % 100 == 0) {
                System.out.println(counter);
            }
            return !record.getIsbn13().isEmpty() && !isbnList.contains(record.getIsbn13());
        }).collect(Collectors.toList());

        List<String> notFoundIsbn = notFoundRecords.stream().map(record -> record.getIsbn13()).collect(Collectors.toList());
        Files.write(Paths.get(CONTENTS_DIR, "\\notFoundIsbn.txt" ), notFoundIsbn);
        System.out.println("#notFoundIsbn = " + notFoundIsbn.size());
        return notFoundRecords;
    }



    private static void updateDB(NielsenDatabaseUpdater updater, List<Record> notFoundRecords, Map<String, Set<String>> missingImages)
            throws ClassNotFoundException, SQLException {
        StringBuilder bookInsert = 
                new StringBuilder(String.format("INSERT INTO book (title, year, source) VALUES %n"))
                .append(updater.createBookInsertValues(notFoundRecords));
        //        System.out.println(INSERT_BOOKS.toString());

        Class.forName("com.mysql.jdbc.Driver");
        try (Connection connection =
                DriverManager
                .getConnection(
                        String.format("%s?user=%s&password=%s", DATABASE_URI, USER, PASSWORD))) {
            {
                PreparedStatement statement = connection.prepareStatement(bookInsert.toString(), Statement.RETURN_GENERATED_KEYS);
                statement.execute();
                ResultSet generatedKeys = statement.getGeneratedKeys();
                List<Integer> keyList = new ArrayList<>();
                while(generatedKeys.next()) {
                    keyList.add(generatedKeys.getInt(1));
                }

                StringBuilder isbnInsert = new StringBuilder(String.format("INSERT INTO identificator (book_id, type, value) VALUES %n"))
                        .append(updater.createISBNInsertValues(notFoundRecords, keyList))
                        .append(String.format(";%n"));
                statement = connection.prepareStatement(isbnInsert.toString());
                statement.execute();

                String descriptionInsertValues = updater.createDescriptionInsertValues(notFoundRecords, keyList);
                if(!descriptionInsertValues.isEmpty()) {
                    StringBuilder descriptionInsert = new StringBuilder(String.format("INSERT INTO description (book_id, type, source, text) VALUES %n"))
                            .append(descriptionInsertValues)
                            .append(String.format(";%n"));
                    statement = connection.prepareStatement(descriptionInsert.toString());
                    statement.execute();
                }

                String imageUpdateInsert = createImageUpdateInsert(notFoundRecords, keyList, missingImages);
                if(!imageUpdateInsert.isEmpty()) {
                    StringBuilder imageInsert = new StringBuilder(String.format("INSERT INTO image (book_id, type, source, path) VALUES %n"))
                            .append(imageUpdateInsert)
                            .append(String.format(";%n"));
                    statement = connection.prepareStatement(imageInsert.toString());
                    statement.execute();
                }
            }
        }

    }

    private static String createImageUpdateInsert(List<Record> records, List<Integer> keyList, Map<String, Set<String>> missingImages) {

        AtomicInteger counter = new AtomicInteger(0);
        List<String> insertValues = new ArrayList<>();

        records.forEach(record -> {
            String fileName = String.join(".", record.getIsbn13(), "jpg");
            if(record.getIsbn13().length() == 0) {
                System.out.println(record.getTitle());
            }
            String subPath = ImageUtil.createSubPath(record.getIsbn13());
            //            System.out.println(subPath + "\\" + fileName);
            //            System.out.println(Paths.get(imageFilePath, "\\small", subPath, "\\", fileName).toString());
            if(missingImages.get("small").contains(fileName)) {
                insertValues.add(String.format("(%d, 'SMALL', 'NIELSEN', '%s')", keyList.get(counter.get()), String.join("/", "small", subPath, fileName)));
            }
            if(missingImages.get("large").contains(fileName)) {
                insertValues.add(String.format("(%d, 'LARGE', 'NIELSEN', '%s')", keyList.get(counter.get()), String.join("/", "large", subPath, fileName)));
            }
            if(missingImages.get("original").contains(fileName)) {
                insertValues.add(String.format("(%d, 'ORIGINAL', 'NIELSEN', '%s')", keyList.get(counter.get()), String.join("/", "original", subPath, fileName)));
            }
            counter.incrementAndGet();
        });

        System.out.println(insertValues.size());

        return insertValues.size() > 0 ? String.join(String.format(",%n"), insertValues) : "";
    }


    public static void main(String... args) throws ClassNotFoundException, IOException, SQLException {

        final long start = System.currentTimeMillis();

        NielsenDatabaseUpdater updater = new NielsenDatabaseUpdater();
        System.out.println("start");

        List<Record> notFoundRecords = findMissingRecords(updater);

       Map<String, Set<String>> missingImages = updater.findMissingImages(notFoundRecords);
        
        System.out.println();

        List<List<Record>> listOfLists = splitList(notFoundRecords);

        listOfLists.forEach(list -> {
            try {
                updateDB(updater, list, missingImages);
            } catch (ClassNotFoundException | SQLException e) {
                e.printStackTrace();
            }
        });
        System.out.println(System.currentTimeMillis() -  start);
        System.out.println("end");
    }
}
