package no.unit.contents;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

public class ContentsDocument {

    public String isbn = "";
    public String title = "";
    public String author = "";
    public String dateOfPublication = "";
    public String tableOfContents = "";
    public String descriptionShort = "";
    public String descriptionLong = "";
    public String review = "";
    public String summary = "";
    public String promotional = "";
    public String imageSmall = "";
    public String imageLarge = "";
    public String imageOriginal = "";
    public String source = "";
    public String modified = "";
    public String created = "";

    public ContentsDocument(String isbn) {
        this.isbn = isbn;
        this.created = Instant.now().minus(2, ChronoUnit.DAYS).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ContentsDocument)) {
            return false;
        }
        ContentsDocument that = (ContentsDocument) o;
        return Objects.equals(author, that.author)
            && Objects.equals(title, that.title)
            && Objects.equals(dateOfPublication, that.dateOfPublication)
            && Objects.equals(modified, that.modified)
            && Objects.equals(created, that.created)
            && Objects.equals(descriptionShort, that.descriptionShort)
            && Objects.equals(descriptionLong, that.descriptionLong)
            && Objects.equals(source, that.source)
            && Objects.equals(isbn, that.isbn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(author,
                title,
                dateOfPublication,
                isbn,
                descriptionShort,
                descriptionLong,
                tableOfContents,
                imageSmall,
                imageLarge,
                imageOriginal,
                modified,
                created,
                source);
    }

}
