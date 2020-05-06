package no.unit.contents;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class NielsenFileReader {

    private static final String CONTENTS_DIR = "/workspace/contents/";
    private static final String IMAGE_DIR = "/workspace/contents/images";

    private void readFiles() {
        final List<String> isbnList = new ArrayList<>();
        System.out.println("Reading ISBN");
        try {
            Files.list(Paths.get(CONTENTS_DIR))
                    .filter(file -> !file.toFile().isDirectory())
                    .forEach(file -> {
                        try {
                            isbnList.addAll(Files.lines(file)
                                    .filter(line -> line.startsWith("<ISBN13>"))
                                    .map(line -> line.replace("<ISBN13>", "").replace("</ISBN13>", ""))
                                    .collect(Collectors.toList()));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Done!");

        System.out.println(isbnList.size());

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
        final List<String> imageList = new ArrayList<String>();
        try {
            AtomicInteger counter = new AtomicInteger();
            imageList.addAll(Files.list(Paths.get(IMAGE_DIR))
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
        System.out.println(imageCount);
        System.out.println(imageList.size());

        long start = System.currentTimeMillis();
        AtomicInteger foundCount = new AtomicInteger();
        List<String> foundImageList =
                imageList.stream().filter(isbn -> {
                    if (foundCount.incrementAndGet() % 1000 == 0) {
                        System.out.println(System.currentTimeMillis() - start);
                        System.out.println(foundCount);
                    }
                    return isbnList.contains(isbn);
                }).collect(Collectors.toList());
        System.out.println(foundImageList.size());
    }

    public static void main(String... args) {
//        new NielsenFileReader().readFiles();
        XmlMapper mapper = new XmlMapper();
//        mapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
        try {
            List<Record> records = new ArrayList<Record>();

            long start = System.currentTimeMillis();
            Files.list(Paths.get(CONTENTS_DIR))
                    .filter(file -> !Files.isDirectory(file))
                    .forEach(file -> {
                        try {
                            records.addAll(mapper.readValue(file.toFile(), NielsenBook.class).getRecord());
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
            System.out.println(records.size());
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
            System.out.printf("brief: %s%n", briefDesc.toString());
            System.out.printf("full: %s%n", fullDesc.toString());
            System.out.printf("toc: %s%n", toc.toString());
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
