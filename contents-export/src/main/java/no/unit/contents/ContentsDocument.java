package no.unit.contents;

import java.util.Objects;

public class ContentsDocument {

    protected String isbn;
    protected String title;
    protected String author;
    protected String dateOfPublication;
    protected String descriptionShort;
    protected String descriptionLong;
    protected String tableOfContents;
    protected String imageSmall;
    protected String imageLarge;
    protected String imageOriginal;
    protected String audioFile;
    protected String source;
    protected String modified;
    protected String created;

    public ContentsDocument(String isbn) {
        this.isbn = isbn;
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
