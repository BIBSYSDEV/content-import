package no.unit.ontents;

import static org.junit.jupiter.api.Assertions.*;

import no.unit.contents.ContentsDatabaseExporter;
import org.junit.jupiter.api.Test;

class ContentsDatabaseExporterTest {


    @Test
    public void testPathManipulation() {
        String url = "small/9781586483678.jpg";
        String expected = "small/8/7/9781586483678.jpg";
        ContentsDatabaseExporter contentsDatabaseExporter = new ContentsDatabaseExporter();
        String newUrl = contentsDatabaseExporter.dealWithOldBIBSYSpath(url);
        assertEquals(expected, newUrl);

        url = "small/8/7/9781586483678.jpg";
        contentsDatabaseExporter = new ContentsDatabaseExporter();
        newUrl = contentsDatabaseExporter.dealWithOldBIBSYSpath(url);
        assertEquals(expected, newUrl);
    }
}