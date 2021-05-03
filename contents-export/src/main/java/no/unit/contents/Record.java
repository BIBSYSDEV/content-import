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
        return year != null ? year : "";
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getTitle() {
        return title != null ? title : "";
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getIsbn13() {
        return isbn13 != null ? isbn13 : "";
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
        return descriptionBrief != null ? descriptionBrief : "";
    }

    public void setDescriptionBrief(String descriptionBrief) {
        this.descriptionBrief = descriptionBrief;
    }

    public String getDescriptionFull() {
        return descriptionFull != null ? descriptionFull : "";
    }

    public void setDescriptionFull(String descriptionFull) {
        this.descriptionFull = descriptionFull;
    }

    public String getTableOfContents() {
        return tableOfContents != null ? tableOfContents : "";
    }

    public void setTableOfContents(String tableOfContents) {
        this.tableOfContents = tableOfContents;
    }

}
