package no.unit.contents;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NielsenFileReader {

    private static final String CONTENTS_DIR = "/pywork/ar/Nielsen datadump april 2021/data/";
    private static final String IMAGE_DIR = "/pywork/ar/Nielsen datadump april 2021/images/";
    private final List<File> fileList = new ArrayList<>();
    private final Set<String> isbnRecordsList = new HashSet<>();
    private final Set<String> isbnImageList = new HashSet<>();

    public Set<String> getIsbnRecordSet() {
        return isbnRecordsList;
    }

    public Set<String> getIsbnImageSet() {
        return isbnImageList;
    }

    public List<File> getFileList() {
        return fileList;
    }

    protected void loadData() {
        System.out.println("Reading ISBN from " + CONTENTS_DIR);
        try (Stream<Path> paths = Files.walk(Paths.get(CONTENTS_DIR))) {
            paths.filter(Files::isRegularFile)
                    .forEach(path -> fileList.add(path.toFile()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        XmlMapper mapper = new XmlMapper();
        mapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
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
                    (int) Files.list(Paths.get(IMAGE_DIR)).filter(file -> !file.toFile().isDirectory()).count();
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
    }

    public List<Record> readFile(File file) {
        XmlMapper mapper = new XmlMapper();
        mapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
        try {
            long start = System.currentTimeMillis();
            System.out.println(start + " Start reading file " + file.getName());
            List<Record> recordList = mapper.readValue(file, NielsenBook.class).getRecord();
            long end = System.currentTimeMillis();
            System.out.println(end + " Read " + recordList.size() + " records from file " + file.getName());
            return recordList;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

}
