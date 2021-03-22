package no.unit.contents;

import java.time.Instant;
import java.util.Objects;

public class ContentsDocument {

    protected String author;
    protected String title;
    protected String dateOfPublication;
    protected String isbn;
    protected String descriptionShort;
    protected String descriptionLong;
    protected String tableOfContents;
    protected String imageSmall;
    protected String imageLarge;
    protected String imageOriginal;
    protected String audioFile;
    protected String source;
    protected Instant modified;
    protected Instant created;
    protected String year;

    /**
     * Creates and IndexDocument with given properties.
     */
    @SuppressWarnings("PMD.ExcessiveParameterList")
    public ContentsDocument(String title,
                            String author,
                            String dateOfPublication,
                            String isbn,
                            String descriptionShort,
                            String descriptionLong,
                            String tableOfContents,
                            String imageSmall,
                            String imageLarge,
                            String imageOriginal,
                            String audioFile,
                            String source,
                            Instant modified,
                            Instant created) {
        this.title = title;
        this.author = author;
        this.dateOfPublication = dateOfPublication;
        this.isbn = isbn;
        this.descriptionShort = descriptionShort;
        this.descriptionLong = descriptionLong;
        this.tableOfContents = tableOfContents;
        this.imageSmall = imageSmall;
        this.imageLarge = imageLarge;
        this.imageOriginal = imageOriginal;
        this.audioFile = audioFile;
        this.source = source;
        this.modified = modified;
        this.created = created;
    }

    public ContentsDocument(String isbn) {
        this.isbn = isbn;
    }

    public String getAuthor() {
        return author;
    }

    public String getTitle() {
        return title;
    }

    public String getDescriptionShort() {
        return descriptionShort;
    }

    public String getDescriptionLong() {
        return descriptionLong;
    }

    public String getTableOfContents() {
        return tableOfContents;
    }

    public String getImageSmall() {
        return imageSmall;
    }

    public String getDateOfPublication() {
        return dateOfPublication;
    }

    public String getImageLarge() {
        return imageLarge;
    }

    public String getImageOriginal() {
        return imageOriginal;
    }

    public String getAudioFile() {
        return audioFile;
    }

    public String getSource() {
        return source;
    }

    public String getIsbn() {
        return isbn;
    }

    public Instant getModified() {
        return modified;
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

    public void setYear(String year) {
        this.year = year;
    }

    public void setTitle(String title) {
    }
}
