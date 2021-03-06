package no.unit.contents;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ImageUtil {

    public static void organizeImages(String path) throws IOException {

        AtomicInteger counter = new AtomicInteger();

        int total = Files.list(Paths.get(path))
        .filter(file -> file.getFileName().toString().endsWith(".jpg"))
        .collect(Collectors.toList()).size();
        
        Files.list(Paths.get(path))
        .filter(file -> file.getFileName().toString().endsWith(".jpg"))
        .forEach(file -> {
            String fileName = file.getFileName().toString().replace(".jpg", "");
            Path dir = Paths.get(path, createSubPath(fileName));
            Path finalFile = Paths.get(path, createSubPath(fileName));
            if(counter.incrementAndGet() % 1000 == 0) {
                System.out.printf("%s\t - %s%n", counter.toString(), Integer.toString(total));
            }
            try {
                Files.createDirectories(dir);
                Files.move(file, finalFile);
//                System.out.printf("%s -> %s%n", file.toString(), finalFile.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public static String createSubPath(String fileName) {
        String firstDirName = fileName.substring(fileName.length() - 1);
        String secondDirName = fileName.substring(fileName.length() - 2, fileName.length() - 1);
        StringBuilder subPath = new StringBuilder("").append(firstDirName).append("/").append(secondDirName);
        return subPath.toString();
    }

    public static void main(String... args) {
        try {
            ImageUtil.organizeImages("E:\\innhold\\Nielsen\\images\\small");
            ImageUtil.organizeImages("E:\\innhold\\Nielsen\\images\\large");
            ImageUtil.organizeImages("E:\\innhold\\Nielsen\\images\\original");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
