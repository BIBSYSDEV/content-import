package no.unit.contents;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;

import java.util.ArrayList;
import java.util.List;

@JsonRootName("data")
public class NielsenBook {

    @JsonProperty
    @JacksonXmlElementWrapper(useWrapping = false)
    List<Record> record = new ArrayList<Record>();

    public List<Record> getRecord() {
        return record;
    }

    public void setRecord(List<Record> record) {
        this.record = record;
    }

    @Override
    public String toString() {
        return "NielsenBook [record=" + record + "]";
    }

}
