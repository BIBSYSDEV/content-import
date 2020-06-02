package no.unit.contents;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.util.StreamReaderDelegate;

public class AlmaDataReader {

    public static void main(String... args ) throws JAXBException, XMLStreamException, FactoryConfigurationError, IOException {

        long start = System.currentTimeMillis();
        Map<String, String> isbnMap = new HashMap<>(); 

        JAXBContext context = JAXBContext.newInstance(Collection.class);
        Set<String> ckbSet = new HashSet<>();;
        Files.list(Paths.get("E:\\innhold\\Nielsen\\data\\almaupdate\\")).filter(file -> file.getFileName().toString().endsWith(".xml")).forEach(file -> {
            System.out.println(file.getFileName().toString());
            InputStream is;
            try {
                is = new FileInputStream(file.toFile());
                try {
                    XMLStreamReader xsr = XMLInputFactory.newFactory().createXMLStreamReader(is);
                    XMLReaderWithoutNamespace xr = new XMLReaderWithoutNamespace(xsr);
                    Unmarshaller unmarshaller = context.createUnmarshaller();

                    List<Map<String, String>> mappingList = parseXml(xr, unmarshaller, ckbSet);

                    System.out.println("mappingList: " + mappingList.size());

                    mappingList.forEach(isbnMap::putAll); 
                } catch (XMLStreamException | FactoryConfigurationError | JAXBException e) {
                    e.printStackTrace();
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        });

        System.out.println("isbnMap: " + isbnMap.size());
        List<Record> records = NielsenDatabaseUpdater.readRecords();

        final Set<String> smallImageMap = new HashSet<>();
        final Set<String> largeImageMap = new HashSet<>();
        final Set<String> originalImageMap = new HashSet<>();
        try {
            Map<String, Set<String>> missingImages = NielsenDatabaseUpdater.findMissingImages(new ArrayList<>(records));
            smallImageMap.addAll(missingImages.get("small").stream().map(imageName -> imageName.replace(".jpg", "")).collect(Collectors.toSet()));
            largeImageMap.addAll(missingImages.get("large").stream().map(imageName -> imageName.replace(".jpg", "")).collect(Collectors.toSet()));
            originalImageMap.addAll(missingImages.get("original").stream().map(imageName -> imageName.replace(".jpg", "")).collect(Collectors.toSet()));
        } catch (SQLException | IOException e1) {
            e1.printStackTrace();
        }
        
        Set<Record> contentsRecords = records.stream().filter(record -> (record.getDescriptionBrief() != null && !record.getDescriptionBrief().isBlank())
                    || (record.getDescriptionFull() != null && !record.getDescriptionFull().isBlank()) 
                    || (record.getTableOfContents() != null && !record.getTableOfContents().isBlank())
                    || (smallImageMap.contains(normalizeIsbn(record.getIsbn13())))
                    || (largeImageMap.contains(normalizeIsbn(record.getIsbn13())))).collect(Collectors.toSet());
        
        System.out.printf("records with contents: %d", contentsRecords.size());

        Set<String> recordIsbnSet = records.stream().map(record -> record.getIsbn13()).collect(Collectors.toSet());

        System.out.println("recordIsbnSet: " + recordIsbnSet.size());

        Set<String> foundSet = new HashSet<>();

        isbnMap.keySet().forEach(isbn -> {
            String convertedIsbn = normalizeIsbn(isbn);
            boolean found = recordIsbnSet.contains(convertedIsbn);
            
            if(found) {
                foundSet.add(normalizeIsbn(isbn));
            }
        });

        System.out.println("found: " + foundSet.size());
        System.out.println("ckbSet: " + ckbSet.size());
        System.out.println(System.currentTimeMillis() - start);

        Set<Record> foundRecords = records.stream().filter(record -> foundSet.contains(normalizeIsbn(record.getIsbn13()))).collect(Collectors.toSet());

        System.out.println(foundRecords.size());

        AtomicInteger counter = new AtomicInteger();

        Collection collection = new Collection();
        collection.setRecordDataRecord(new ArrayList<>());

        System.out.println("small images: " + smallImageMap.size());
        System.out.println("large images: " + largeImageMap.size());

        AtomicInteger total = new AtomicInteger();
        
        foundRecords.stream()
        .forEach(record -> {

            if((record.getDescriptionBrief() != null && !record.getDescriptionBrief().isBlank())
                    || (record.getDescriptionFull() != null && !record.getDescriptionFull().isBlank()) 
                    || (record.getTableOfContents() != null && !record.getTableOfContents().isBlank())
                    || (smallImageMap.contains(record.getIsbn13()))
                    || (largeImageMap.contains(record.getIsbn13()))) {

                total.incrementAndGet();
                
                RecordDataRecord dataRecord = new RecordDataRecord();

                List<Controlfield> controlfields = new ArrayList<>();
                Controlfield controlfield = new Controlfield();
                controlfield.setTag("001");
                String isbn13 = record.getIsbn13();
                controlfield.setValue(isbnMap.get(isbn13));
                controlfields.add(controlfield);
                dataRecord.setControlfield(controlfields);

                ArrayList<Datafield> datafields = new ArrayList<>();
                dataRecord.setDatafield(datafields);
                if(record.getDescriptionBrief() != null && !record.getDescriptionBrief().isBlank()){
                    String shortDescription = "Forlagets beskrivelse (kort)";
                    String value = String.format("https://contents.bibsys.no/content/%s?type=DESCRIPTION_SHORT", record.getIsbn13());
                    Datafield datafield = createSubfieldDescription(shortDescription, value, false);
                    datafields.add(datafield);
                }

                if(record.getDescriptionFull() != null && !record.getDescriptionFull().isBlank()){
                    String longDescription = "Forlagets beskrivelse (lang)";
                    String value = String.format("https://contents.bibsys.no/content/%s?type=DESCRIPTION_LONG", record.getIsbn13());
                    Datafield datafield = createSubfieldDescription(longDescription, value, false);
                    datafields.add(datafield);
                }

                if(record.getTableOfContents() != null && !record.getTableOfContents().isBlank()){
                    String tableOfContentsDescription = "Innholdsfortegnelse";
                    String value = String.format("https://contents.bibsys.no/content/%s?type=CONTENTS", record.getIsbn13());
                    Datafield datafield = createSubfieldDescription(tableOfContentsDescription, value, false);
                    datafields.add(datafield);
                }

                String secondLinkPart = isbn13.substring(isbn13.length() - 2, isbn13.length() - 1);
                String firstLinkPart = isbn13.substring(isbn13.length() - 1);

                // images
                if(smallImageMap.contains(isbn13)) {
                    String longDescription = "Miniatyrbilde";
                    String value = String.format("https://contents.bibsys.no/content/images/small/%s/%s/%s.jpg", firstLinkPart, secondLinkPart, isbn13);
                    Datafield datafield = createSubfieldDescription(longDescription, value, true);
                    datafields.add(datafield);
                }

                if(largeImageMap.contains(isbn13)) {
                    String longDescription = "Omslagsbilde";
                    String value = String.format("https://contents.bibsys.no/content/images/large/%s/%s/%s.jpg", firstLinkPart, secondLinkPart, isbn13);
                    Datafield datafield = createSubfieldDescription(longDescription, value, true);
                    datafields.add(datafield);
                }


                collection.getRecordDataRecord().add(dataRecord);

                if(counter.incrementAndGet() == 50000) {

                    try {
                        writeToFile(collection);
                    } catch (JAXBException e) {
                        e.printStackTrace();
                    }
                    counter.set(0);
                    collection.getRecordDataRecord().clear();
                }
            }
        });

        if(collection.getRecordDataRecord().size() > 0 && collection.getRecordDataRecord().size() < 50000) {
            try {
                writeToFile(collection);
            } catch (JAXBException e) {
                e.printStackTrace();
            }
        }
        
        System.out.printf("total: " + total.get());
    }

    private static String normalizeIsbn(String isbn) {
        String convertedIsbn = isbn.replace("-", "");
        if(convertedIsbn.length() >= 10) {
            convertedIsbn = convertedIsbn.length() == 13 ? convertedIsbn : isbn10toIsbn13(convertedIsbn);
        }
        return convertedIsbn;
    }

    private static Datafield createSubfieldDescription(String description, String value, boolean isImage) {
        Datafield datafield = new Datafield();
        datafield.setSubfield(new ArrayList<>());
        datafield.setTag("956");
        datafield.setInd1("4");
        datafield.setInd2("2");

        {
            Subfield subfield3 = new Subfield();
            subfield3.setCode("3");
            subfield3.setValue(description);
            datafield.getSubfield().add(subfield3);
        }

        {
            Subfield subfielda = new Subfield();
            subfielda.setCode("u");
            subfielda.setValue(value);
            datafield.getSubfield().add(subfielda);
        }

        if(isImage)
        {
            Subfield subfielda = new Subfield();
            subfielda.setCode("q");
            subfielda.setValue("image/jpeg");
            datafield.getSubfield().add(subfielda);
        }

        return datafield;
    }

    private static void writeToFile(Collection collection) throws JAXBException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd");
        String today = dateFormat.format(new Date());
        String fileName = "e_content_import_" +  today + "_" + System.currentTimeMillis() + ".xml";

        System.out.println(fileName);

        JAXBContext jaxbContext = JAXBContext.newInstance(Collection.class);
        Marshaller marshaller = jaxbContext.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(collection, new File(fileName));
        //        marshaller.marshal(collection, System.out);
    }

    private static List<Map<String, String>> parseXml(XMLReaderWithoutNamespace xr, Unmarshaller unmarshaller, Set<String> ckbSet)
            throws JAXBException {
        Collection collection = unmarshaller.unmarshal(xr, Collection.class).getValue();

        AtomicInteger ckbCounter = new AtomicInteger();

        if(collection != null && collection.getRecordDataRecord() != null) {

            collection.recordDataRecord.forEach(record ->
            { 
                if(record.getDatafield() != null) {
                    AtomicBoolean hasCkb = new AtomicBoolean(false);
                    record.getDatafield().forEach(datafield -> {

                        if(datafield.getTag() != null) {
                            if(datafield.getTag().equals("035")) {
                                datafield.getSubfield().forEach(subfield -> {
                                    String code = subfield.getCode();
                                    String value = subfield.getValue();
                                    if(value.toLowerCase().startsWith("(ckb)")) {
                                        hasCkb.getAndSet(true);
                                    }
                                });
                            }
                        }
                    });
                    if(hasCkb.get()) {
                        ckbCounter.incrementAndGet();
                        ckbSet.add(record.getControlfield().stream().filter(controlfield -> controlfield.getTag().equals("001")).findFirst().map(controlfield -> controlfield.getValue()).get());
                    }
                }
            });
            System.out.println(ckbSet.size());
            List<RecordDataRecord> filteredRecord = filterOn020(collection);

            System.out.println("filteredRecords:" + filteredRecord.size());
            System.out.println("ckb: " + ckbCounter.toString());


            System.out.println("----------------------------");

            List<Map<String, String>> mappingList = filteredRecord.stream().map(record -> {

                String mmsId = record.getControlfield().stream()
                        .filter(controlfield -> controlfield.getTag().contentEquals("001"))
                        .findFirst().get().getValue();

                List<Datafield> datafieldList = record.getDatafield().stream()
                        .filter(datafield -> datafield.getTag().equals("020"))
                        .collect(Collectors.toList());

                Set<String> isbnSet = new HashSet<>();
                datafieldList.forEach(datafield -> {
                    datafield.getSubfield().forEach(subfield -> isbnSet.add(subfield.getCode().equals("a") ? subfield.getValue() : null));
                });

                Map<String, String> isbnMapping = new HashMap<>();
                isbnSet.forEach(isbn -> {
                    if(isbn != null) { 
                        if(ckbSet.contains(mmsId)) {
                            isbnMapping.put(normalizeIsbn(isbn), mmsId);
                        }
                    }
                });

                return isbnMapping;
            }).collect(Collectors.toList());
            return mappingList;
        }
        return new ArrayList<>();
    }

    private static List<RecordDataRecord> filterOn020(Collection collection) {
        List<RecordDataRecord> filteredRecord = collection.getRecordDataRecord().stream()
                .filter(record -> record.getDatafield() != null && !record.getDatafield().isEmpty()).collect(Collectors.toList());

        filteredRecord.forEach(record -> {
            List<Datafield> datafields = record.getDatafield().stream().filter(datafield -> datafield.getTag().equals("020")).collect(Collectors.toList());
            record.getDatafield().clear();
            if(datafields != null && !datafields.isEmpty()) {
                datafields.forEach(datafield -> {
                    List<Subfield> subfields = datafield.getSubfield().stream().filter(subfield -> subfield.getCode().equals("a")).collect(Collectors.toList());
                    datafield.getSubfield().clear();
                    if(subfields != null && !subfields.isEmpty()) {
                        datafield.getSubfield().addAll(subfields);
                    }
                });
                record.getDatafield().addAll(datafields.stream().filter(datafield -> !datafield.getSubfield().isEmpty()).collect(Collectors.toList()));
            }
        });

        filteredRecord = filteredRecord.stream().filter(record -> !record.getDatafield().isEmpty()).collect(Collectors.toList());

        return filteredRecord;
    }

    public static String isbn13to10convert(String isbn13)//ISBN-13 to ISBN-10
    {
        int i, k, j = 0;
        int count = 10;
        String st=isbn13.trim().substring(3,12);
        for(i = 0; i < st.length(); i++)
        {
            k = Integer.parseInt(st.charAt(i) + "");
            j = j + k*count;
            count--;
        }
        j = (11 - (j % 11)) % 11;
        return st + "" + j;
    }

    public static String isbn10toIsbn13( String isbn10 ) {
        String isbn13  = isbn10;
        isbn13 = "978" + isbn13.substring(0,9);
        int d;

        int sum = 0;
        for (int i = 0; i < isbn13.length(); i++) {
            d = ((i % 2 == 0) ? 1 : 3);
            sum += ((((int) isbn13.charAt(i)) - 48) * d);
        }
        sum = (10 - (sum % 10)) % 10;
        isbn13 += sum;

        return isbn13;
    }


    public static class XMLReaderWithoutNamespace extends StreamReaderDelegate {
        public XMLReaderWithoutNamespace(XMLStreamReader reader) {
            super(reader);
        }
        @Override
        public String getAttributeNamespace(int arg0) {
            return "";
        }
        @Override
        public String getNamespaceURI() {
            return "";
        }
    }


    @XmlRootElement(name = "collection")
    @XmlAccessorType (XmlAccessType.FIELD)
    public static class Collection {

        @Override
        public String toString() {
            return "Collection [recordDataRecord=" + recordDataRecord + "]";
        }

        @XmlElement(name = "record")
        private List<RecordDataRecord> recordDataRecord = null;

        public List<RecordDataRecord> getRecordDataRecord() {
            return recordDataRecord;
        }

        public void setRecordDataRecord(List<RecordDataRecord> recordDataRecord) {
            this.recordDataRecord = recordDataRecord;
        }
    }

    @XmlRootElement(name = "record")
    @XmlAccessorType (XmlAccessType.FIELD)
    public static class RecordDataRecord {

        @XmlElement(name = "leader")
        private String leader;

        @XmlElement(name = "controlfield")
        private List<Controlfield> controlfield = null;

        @XmlElement(name = "datafield")
        private List<Datafield> datafield = null;

        @Override
        public String toString() {
            return "RecordDataRecord [leader=" + leader + ", controlfield=" + controlfield + ", datafield=" + datafield
                    + "]";
        }

        public List<Controlfield> getControlfield() {
            return controlfield;
        }

        public void setControlfield(List<Controlfield> controlfield) {
            this.controlfield = controlfield;
        }

        public String getLeader() {
            return leader;
        }

        public void setLeader(String leader) {
            this.leader = leader;
        }

        public List<Datafield> getDatafield() {
            return datafield;
        }

        public void setDatafield(List<Datafield> datafield) {
            this.datafield = datafield;
        }
    }


    @XmlRootElement(name = "controlfield")
    @XmlAccessorType (XmlAccessType.FIELD)
    public static class Controlfield {

        @Override
        public String toString() {
            return "Controlfield [tag=" + tag + ", value=" + value + "]\n";
        }

        @XmlAttribute(name = "tag")
        private String tag;

        @XmlValue
        private String value;

        public String getTag() {
            return tag;
        }

        public void setTag(String tag) {
            this.tag = tag;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    @XmlRootElement(name = "datafield")
    @XmlAccessorType (XmlAccessType.FIELD)
    public static class Datafield {

        @Override
        public String toString() {
            return "Datafield [subfield=" + subfield + ", tag=" + tag + ", ind1=" + ind1 + ", ind2=" + ind2 + "]";
        }

        @XmlElement(name = "subfield")
        private List<Subfield> subfield;

        public List<Subfield> getSubfield() {
            return subfield;
        }

        public void setSubfield(List<Subfield> subfield) {
            this.subfield = subfield;
        }

        @XmlAttribute(name = "tag")
        private String tag;

        public String getTag() {
            return tag;
        }

        public void setTag(String tag) {
            this.tag = tag;
        }

        public String getInd1() {
            return ind1;
        }

        public void setInd1(String ind1) {
            this.ind1 = ind1;
        }

        public String getInd2() {
            return ind2;
        }

        public void setInd2(String ind2) {
            this.ind2 = ind2;
        }

        @XmlAttribute(name = "ind1")
        private String ind1;

        @XmlAttribute(name = "ind2")
        private String ind2;

    }

    @XmlRootElement(name = "subfield")
    @XmlAccessorType (XmlAccessType.FIELD)
    public static class Subfield {
        @XmlAttribute(name = "code")
        private String code;

        @XmlValue
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getCode() {
            return code;
        }

        @Override
        public String toString() {
            return "Subfield [code=" + code + ", value=" + value + "]";
        }

        public void setCode(String code) {
            this.code = code;
        }
    }

    //    @XmlRootElement(name = "searchRetrieveResponse")
    //    @XmlAccessorType (XmlAccessType.FIELD)
    //    public static class Response {
    //        
    //        @Override
    //        public String toString() {
    //            return "Response [xmlns=" + xmlns + ", version=" + version + ", numberOfRecords=" + numberOfRecords
    //                    + ", records=" + records + "]";
    //        }
    //
    //        @XmlAttribute(name = "xmlns")
    //        private String xmlns;
    //        
    //        public String getXmlns() {
    //            return xmlns;
    //        }
    //
    //        public void setXmlns(String xmlns) {
    //            this.xmlns = xmlns;
    //        }
    //
    //        @XmlElement(name = "version")
    //        private String version;
    //        
    //        @XmlElement(name = "numberOfRecords")
    //        private String numberOfRecords;
    //        
    //        public String getVersion() {
    //            return version;
    //        }
    //
    //        public void setVersion(String version) {
    //            this.version = version;
    //        }
    //
    //        public String getNumberOfRecords() {
    //            return numberOfRecords;
    //        }
    //
    //        public void setNumberOfRecords(String numberOfRecords) {
    //            this.numberOfRecords = numberOfRecords;
    //        }
    //
    //        public Records getRecords() {
    //            return records;
    //        }
    //
    //        public void setRecords(Records records) {
    //            this.records = records;
    //        }
    //
    //        @XmlElement(name = "records")
    //        private Records records;
    //        
    //    }
    //    
    //
    //    @XmlRootElement(name = "records")
    //    @XmlAccessorType (XmlAccessType.FIELD)
    //    public static class Records {
    //        
    //        @XmlElement(name = "record")
    //        private List<Record> record = null;
    //
    //        @Override
    //        public String toString() {
    //            return "Records [record=" + record + "]";
    //        }
    //
    //        public List<Record> getRecord() {
    //            return record;
    //        }
    //
    //        public void setRecord(List<Record> record) {
    //            this.record = record;
    //        }
    //    }
    //
    //    
    //    @XmlRootElement(name = "record")
    //    @XmlAccessorType (XmlAccessType.FIELD)
    //    public static class Record {
    //        
    //        @Override
    //        public String toString() {
    //            return "Record [recordSchema=" + recordSchema + ", recordPacking=" + recordPacking + ", recordData="
    //                    + recordData + ", recordIdentifier=" + recordIdentifier + ", recordPosition=" + recordPosition
    //                    + "]";
    //        }
    //
    //        @XmlElement(name = "recordSchema")
    //        private String recordSchema;
    //        
    //        @XmlElement(name = "recordPacking")
    //        private String recordPacking;
    //        
    //        @XmlElement(name = "recordData")
    //        private Collection recordData;
    //
    //        @XmlElement(name = "recordIdentifier")
    //        private Collection recordIdentifier;
    //
    //        @XmlElement(name = "recordPosition")
    //        private Collection recordPosition;
    //        
    //        public Collection getRecordIdentifier() {
    //            return recordIdentifier;
    //        }
    //
    //        public void setRecordIdentifier(Collection recordIdentifier) {
    //            this.recordIdentifier = recordIdentifier;
    //        }
    //
    //        public Collection getRecordPosition() {
    //            return recordPosition;
    //        }
    //
    //        public void setRecordPosition(Collection recordPosition) {
    //            this.recordPosition = recordPosition;
    //        }
    //
    //        public String getRecordSchema() {
    //            return recordSchema;
    //        }
    //
    //        public void setRecordSchema(String recordSchema) {
    //            this.recordSchema = recordSchema;
    //        }
    //
    //        public String getRecordPacking() {
    //            return recordPacking;
    //        }
    //
    //        public void setRecordPacking(String recordPacking) {
    //            this.recordPacking = recordPacking;
    //        }
    //
    //        public Collection getRecordData() {
    //            return recordData;
    //        }
    //
    //        public void setRecordData(Collection recordData) {
    //            this.recordData = recordData;
    //        }
    //    }

}
