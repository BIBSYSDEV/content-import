package no.unit.ontents;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import no.unit.contents.ContentsDatabaseExporter;
import no.unit.contents.ContentsDocument;
import org.apache.commons.text.StringEscapeUtils;
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

    @Test
    public void testDescriptionMangling() throws SQLException {
        String input = "Boken dekker behovet for en grunnleggende innf&oslash;ringsbok i etikk for de &oslash;"
            + "konomisk/administrative studier";
        ContentsDocument doc = new ContentsDocument("isbn");
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        ResultSet res = mock(ResultSet.class);
        when(mockStatement.executeQuery()).thenReturn(res);
        when(res.next()).thenReturn(true, false);
        when(res.getString(ContentsDatabaseExporter.COLUMN_TYPE)).thenReturn(
            ContentsDatabaseExporter.DESCRIPTION_SHORT_TYPE);
        when(res.getString(ContentsDatabaseExporter.COLUMN_TEXT)).thenReturn(input);
        ContentsDatabaseExporter contentsDatabaseExporter = new ContentsDatabaseExporter();
        contentsDatabaseExporter.findDescriptionData(mockStatement, doc);
        assertFalse(doc.descriptionShort.contains("&oslash;"));
    }
}