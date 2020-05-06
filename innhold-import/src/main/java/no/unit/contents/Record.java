package no.unit.contents;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Record {

//    public Record(String isbn13) {
//    }

    @JsonProperty("ISBN13")
    private String isbn13;

    @JsonProperty("FTS")
    private String title = "";

    @JsonProperty("PUBPD")
    private String year;

    @JsonProperty("NBDFSD")
    private String descriptionBrief;

    @JsonProperty("NBDFLD")
    private String descriptionFull;

    @JsonProperty("NBDFTOC")
    private String tableOfContents;

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getIsbn13() {
        return isbn13;
    }

    public void setIsbn13(String isbn13) {
        this.isbn13 = isbn13;
    }

    @Override
    public String toString() {
        return String.format(
                "Record [isbn13=%s, title=%s, year=%s, descriptionBrief=%s, descriptionFull=%s, tableOfContents=%s]%n",
                isbn13, title.substring(0, title.length() > 10 ? 10 : title.length()), year,
                descriptionBrief != null
                        ? descriptionBrief.substring(0, descriptionBrief.length() > 10 ? 10 : descriptionBrief.length())
                        : "",
                descriptionFull != null
                        ? descriptionFull.substring(0, descriptionFull.length() > 10 ? 10 : descriptionFull.length())
                        : "",
                tableOfContents != null
                        ? tableOfContents.substring(0, tableOfContents.length() > 10 ? 10 : tableOfContents.length())
                        : "");
    }

    public String getDescriptionBrief() {
        return descriptionBrief;
    }

    public void setDescriptionBrief(String descriptionBrief) {
        this.descriptionBrief = descriptionBrief;
    }

    public String getDescriptionFull() {
        return descriptionFull;
    }

    public void setDescriptionFull(String descriptionFull) {
        this.descriptionFull = descriptionFull;
    }

    public String getTableOfContents() {
        return tableOfContents;
    }

    public void setTableOfContents(String tableOfContents) {
        this.tableOfContents = tableOfContents;
    }

//        @XmlAnyElement
//        public Element[] others;
//
//        public Element[] getOthers() {
//            return others;
//        }
//
//        public void setOthers(Element[] others) {
//            this.others = others;
//        }

//        <ISBN13>9780864430793</ISBN13><ISBN13H>978-0-86443-079-3</ISBN13H><ISBN13S>978 0 86443 079 3</ISBN13S><FTS>
//        The Van
//        Der Most Report:
//        a P.
//        I.D.View of Soekarno'sP.N.I.</FTS><CR1>A01</CR1>
//
//        <CRT1>By (author)</CRT1>
//        <CCI1>N</CCI1>
//        <CNI1>Most, B. R. van der</CNI1>
//        <CR1>A01</CR1>
//        <CRT1>By (author)</CRT1>
//        <CCI1>N</CCI1>
//        <ICFN1>B. R. van der</ICFN1>
//        <ICKN1>Most</ICKN1>
//        <PFC>MC</PFC>
//        <PFCT>Microfilm</PFCT>
//        <IMPN>James Cook University of North Queensland</IMPN>
//        <PUBN>James Cook University of North Queensland</PUBN>
//        <POP>Townsville, QLD</POP>
//        <COP>Australia</COP>
//        <DEWS1>DC19</DEWS1>
//        <DEWEY1>324.259803</DEWEY1>

}
