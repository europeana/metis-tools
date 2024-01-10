package eu.europeana.metis.processor.utilities;

/**
 * The type Report row.
 */
public class ReportRow {
    private String recordId;
    private String imageLink;
    private String imageLinkHex;
    private String status;
    private long elapsedTime;
    private long widthBefore;
    private long heightBefore;
    private long widthAfter;
    private long heightAfter;

    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    public String getImageLink() {
        return imageLink;
    }

    public void setImageLink(String imageLink) {
        this.imageLink = imageLink;
    }

    public String getImageLinkHex() {
        return imageLinkHex;
    }

    public void setImageLinkHex(String imageLinkHex) {
        this.imageLinkHex = imageLinkHex;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public long getWidthBefore() {
        return widthBefore;
    }

    public void setWidthBefore(long widthBefore) {
        this.widthBefore = widthBefore;
    }

    public long getHeightBefore() {
        return heightBefore;
    }

    public void setHeightBefore(long heightBefore) {
        this.heightBefore = heightBefore;
    }

    public long getWidthAfter() {
        return widthAfter;
    }

    public void setWidthAfter(long widthAfter) {
        this.widthAfter = widthAfter;
    }

    public long getHeightAfter() {
        return heightAfter;
    }

    public void setHeightAfter(long heightAfter) {
        this.heightAfter = heightAfter;
    }

    @Override
    public String toString() {
        return recordId +
                "," + String.format("\"%s\"",imageLink) +
                "," + imageLinkHex +
                "," + status +
                "," + elapsedTime +
                "," + widthBefore +
                "," + heightBefore +
                "," + widthAfter +
                "," + heightAfter;
    }
}
