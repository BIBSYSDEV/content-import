package no.unit.contents;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NielsenFileReader {

    private static final String CONTENTS_DIR = "/pywork/ar/Nielsen datadump april 2021/data/";
    private static final String IMAGE_DIR = "/pywork/ar/Nielsen datadump april 2021/images/";
    private final List<String> isbnRecordsList = new ArrayList<>();
    private final List<String> isbnImageList = new ArrayList<>();
    private final List<Record> records = new ArrayList<>();
    private final Map<String, Record> recordsMap = new HashMap<>();

    public Set<String> getIsbnRecordsSortedSet() {
        return new TreeSet<>(isbnRecordsList);
    }

    public Set<String> getIsbnImagesSortedSet() {
        return new TreeSet<>(isbnImageList);
    }

    public Map<String, Record> getRecordMap() {
        return recordsMap;
    }

    private void loadIsbns() {
        System.out.println("Reading ISBN from " + CONTENTS_DIR);
        try (Stream<Path> paths = Files.walk(Paths.get(CONTENTS_DIR))) {
            paths.filter(Files::isRegularFile)
                    .forEach(System.out::println);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Files.list(Paths.get(CONTENTS_DIR))
                    .filter(file -> !file.toFile().isDirectory())
                    .forEach(file -> {
                        System.out.println("Reading file: " + file.getFileName());
                        if (file.getFileName().toString().endsWith(".add")) {
                            System.out.println("Parsing file: " + file.getFileName());
                            try {
                                isbnRecordsList.addAll(Files.lines(file)
                                        .filter(line -> line.startsWith("<ISBN13>"))
                                        .map(line -> line.replace("<ISBN13>", "").replace("</ISBN13>", ""))
                                        .collect(Collectors.toList()));
                            } catch (IOException e) {
                                System.out.println("Failed while reading file: " + file.getFileName());
                                e.printStackTrace();
                            }
                        }
                    });
        } catch (IOException e) {
            System.out.println("Not good reading metadata-files.");
            e.printStackTrace();
        }

        System.out.println("Done!");

        System.out.println("No of isbn from metadata files: " + isbnRecordsList.size());

        if (isbnRecordsList.isEmpty()) {
            System.out.println("We stop since we were not able to read metadata.");
            System.exit(1);
        }

        System.out.println("Counting images");
        int imageCount = -1;
        try {
            imageCount =
                    Files.list(Paths.get(IMAGE_DIR)).filter(file -> !file.toFile().isDirectory())
                            .collect(Collectors.toList()).size();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        System.out.println("Done!");
        System.out.println(imageCount);

        System.out.println("Finding images");
        try {
            AtomicInteger counter = new AtomicInteger();
            isbnImageList.addAll(Files.list(Paths.get(IMAGE_DIR))
                    .filter(file -> !file.toFile().isDirectory())
//                    .filter(file -> isbnList.contains(file.getFileName().toString().toLowerCase().replace(".jpg", "")))
                    .map(file -> {
                        if (counter.incrementAndGet() % 1000 == 0) {
                            System.out.println(counter);
                        }
                        return file.getFileName().toString().toLowerCase().replace(".jpg", "");
                    })
                    .collect(Collectors.toList()));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("Done!");
        System.out.println("No of image isbns: " + imageCount);
        System.out.println(isbnImageList.size());

        long start = System.currentTimeMillis();
        AtomicInteger foundCount = new AtomicInteger();
        List<String> foundImageList =
                isbnImageList.stream().filter(isbn -> {
                    if (foundCount.incrementAndGet() % 1000 == 0) {
//                        System.out.println(System.currentTimeMillis() - start);
                        System.out.println(foundCount);
                    }
                    return isbnRecordsList.contains(isbn);
                }).collect(Collectors.toList());
        System.out.println("Found image size List: " + foundImageList.size());
    }

    public void processFiles() {
        this.loadIsbns();
        XmlMapper mapper = new XmlMapper();
        
        mapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
        try {

            long start = System.currentTimeMillis();
            Files.list(Paths.get(CONTENTS_DIR))
                    .filter(file -> !Files.isDirectory(file))
                    .forEach(file -> {
                        try {
                            List<Record> recordSubList = mapper.readValue(file.toFile(), NielsenBook.class).getRecord();
                            records.addAll(recordSubList);
                            for (Record record : recordSubList) {
                                recordsMap.put(record.getIsbn13(), record);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
//            AtomicInteger imageCounter = new AtomicInteger();
//            int imageCount =
//                    records.stream()
//                            .filter(record -> {
//                                if (imageCounter.incrementAndGet() % 1000 == 0)
//                                    System.out.println(imageCounter);
//                                return Files
//                                        .exists(Paths.get(String.format("%s/%s.jpg", IMAGE_DIR, record.getIsbn13())));
//                            })
//                            .collect(Collectors.toList()).size();
            System.out.println(System.currentTimeMillis() - start);
            System.out.println("No of records: " + records.size());
            AtomicInteger briefDesc = new AtomicInteger();
            AtomicInteger fullDesc = new AtomicInteger();
            AtomicInteger toc = new AtomicInteger();
            records.forEach(record -> {
                if (record.getDescriptionBrief() != null)
                    briefDesc.incrementAndGet();
                if (record.getDescriptionFull() != null)
                    fullDesc.incrementAndGet();
                if (record.getTableOfContents() != null)
                    toc.incrementAndGet();
            });
            System.out.printf("brief: %s%n", briefDesc);
            System.out.printf("full: %s%n", fullDesc);
            System.out.printf("toc: %s%n", toc);
//            System.out.println(imageCount);

            List<Record> noDescription =
                    records.stream()
                            .filter(record -> record.getDescriptionBrief() == null
                                    && record.getDescriptionFull() == null && record.getTableOfContents() == null)
                            .collect(Collectors.toList());
            System.out.println(noDescription.size());
            System.out.println(noDescription.stream()
                    .filter(record -> !Files
                            .exists(Paths.get(String.format("%s/%s.jpg", IMAGE_DIR, record.getIsbn13()))))
                    .collect(Collectors.toList()).size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
