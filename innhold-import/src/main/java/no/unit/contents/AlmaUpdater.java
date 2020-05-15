package no.unit.contents;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.MediaType;

import org.glassfish.jersey.client.JerseyClientBuilder;

public class AlmaUpdater {

    public static void main(String... argss ) throws IOException {

        long start = System.currentTimeMillis();
        
        System.out.println("starting");
        
        List<Record> records = NielsenDatabaseUpdater.readRecords();

        System.out.println("records = " + records.size());

        String sruQuery = "http://bibsys.alma.exlibrisgroup.com/view/sru/47BIBSYS_NETWORK?";
        String queryString = "version=1.2&operation=searchRetrieve&recordSchema=marcxml&maximumRecords=50&query=alma.isbn=";

        Set<String> isbnSet = records.stream().map(record -> record.getIsbn13()).collect(Collectors.toSet());

        Set<Set<String>> splitSet = new HashSet<>();

        Set<String> tempSet = new HashSet<>();
        isbnSet.forEach(isbn -> {
            tempSet.add(isbn);
            if(tempSet.size() == 50) {
                splitSet.add(new HashSet<String>(tempSet));
                tempSet.clear();
            }
        });

        System.out.println("splitSet: " + splitSet.size());

        Set<String> sruSet = splitSet.stream().map(set -> {
            return new StringBuilder(sruQuery).append(queryString + String.join("%20or%20alma.isbn=", set)).toString();
        }).collect(Collectors.toSet());

        Client client = JerseyClientBuilder.newClient();

//        String sruUrl = (String) sruSet.toArray()[0];
//        System.out.println(sruUrl);

        AtomicInteger counter = new AtomicInteger();
        
        String fileName = "isbn_" + System.currentTimeMillis() + ".xml";
        sruSet.forEach(sruUrl ->
        {
            String result = client.target(sruUrl)
                    .request()
                    .accept(MediaType.APPLICATION_XML)
                    .buildGet()
                    .invoke(String.class);
            
            List<String> lines = result.lines().collect(Collectors.toList());
            
            try {
                Files.write(Paths.get(fileName), lines, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            } catch (IOException e) {
                e.printStackTrace();
            }
            
//            System.out.println(result.lines().filter(line -> line.contains("tag=\"001\"")).collect(Collectors.toSet()).size());
//            result.lines().filter(line -> line.contains("tag=\"001\""))
//                .map(line -> line.replace("          <controlfield tag=\"001\">", "").replace("</controlfield>", ""))
//                .collect(Collectors.toList())
//                .forEach(System.out::println);
            System.out.println("" + Math.floor((System.currentTimeMillis() - start)/1000) + " - " + (sruSet.size() - counter.getAndIncrement()));
        });

        System.out.println(System.currentTimeMillis() - start);
        System.out.println("end");
    }

}
