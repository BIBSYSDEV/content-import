package no.unit.contents;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
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

import no.unit.contents.SruResponse.Datafield;

public class SruResponse {

    public static void main(String... args ) throws JAXBException, XMLStreamException, FactoryConfigurationError, IOException {

        long start = System.currentTimeMillis();
        Map<String, String> isbnMap = new HashMap<>(); 

        JAXBContext context = JAXBContext.newInstance(Collection.class);
        Files.list(Paths.get("E:\\innhold\\Nielsen\\data\\almaupdate\\")).filter(file -> file.getFileName().toString().endsWith(".xml")).forEach(file -> {
            System.out.println(file.getFileName().toString());
            InputStream is;
            try {
                is = new FileInputStream(file.toFile());
                try {
                    XMLStreamReader xsr = XMLInputFactory.newFactory().createXMLStreamReader(is);
                    XMLReaderWithoutNamespace xr = new XMLReaderWithoutNamespace(xsr);
                    Unmarshaller unmarshaller = context.createUnmarshaller();

                    List<Map<String, String>> mappingList = parseXml(xr, unmarshaller);

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

        Set<String> recordIsbnSet = records.stream().map(record -> record.getIsbn13()).collect(Collectors.toSet());

        System.out.println("recordIsbnSet: " + recordIsbnSet.size());

        Set<String> foundMap = new HashSet<>();

        isbnMap.keySet().forEach(isbn -> {
            String convertedIsbn = isbn.replace("-", "");
            if(convertedIsbn.length() >= 10) {
                convertedIsbn = convertedIsbn.length() == 13 ? convertedIsbn : isbn10toIsbn13(convertedIsbn);
                boolean found = recordIsbnSet.contains(convertedIsbn);

                if(found) {
                    foundMap.add(isbn);
                }
            }
        });


        System.out.println("found: " + foundMap.size());
        System.out.println(System.currentTimeMillis() - start);
    }

    private static List<Map<String, String>> parseXml(XMLReaderWithoutNamespace xr, Unmarshaller unmarshaller)
            throws JAXBException {
        Collection collection = unmarshaller.unmarshal(xr, Collection.class).getValue();

        if(collection != null && collection.getRecordDataRecord() != null) {


            List<RecordDataRecord> filteredRecord = filterOn020(collection);

            System.out.println(filteredRecord.size());


            filteredRecord.forEach(record -> record.getDatafield().forEach(datafield -> {

                if(datafield.getTag().equals("020")) {
                    System.out.print(datafield.getTag());
                    datafield.getSubfield().forEach(subfield -> System.out.print(" " + subfield.getCode() + " " + subfield.getValue()));
                    System.out.println();
                }
            }));

            System.out.println("----------------------------");

            List<Map<String, String>> mappingList = filteredRecord.stream().map(record -> {
                Map<String, String> isbnMapping = new HashMap<>();

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

                isbnSet.forEach(isbn -> {
                    if(isbn != null) {
                        isbnMapping.put(isbn, mmsId);
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
