package eu.europeana.metis.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import dev.morphia.annotations.*;
import eu.europeana.metis.mongo.utils.ObjectIdSerializer;
import org.bson.types.ObjectId;

import java.time.LocalDateTime;

@Entity
@Indexes({
        @Index(fields = {@Field("country")}),
        @Index(fields = {@Field("3D")}),
        @Index(fields = {@Field("highQuality")}),
        @Index(fields = {@Field("totalRecords")}),
        @Index(fields = {@Field("timestamp")}),
        @Index(fields = {@Field("country"),
                @Field("3D"),
                @Field("highQuality"),
                @Field("totalRecords"),
                @Field("timestamp")})
})
public class Historical {

    @Id
    @JsonSerialize(using = ObjectIdSerializer.class)
    private ObjectId id;

    @Property("country")
    private String country;

    @Property("3D")
    private int threeD;

    @Property("highQuality")
    private int highQuality;

    @Property("totalRecords")
    private int totalRecords;

    @Property("timestamp")
    private LocalDateTime timestamp;

    //Empty constructor for when we perform queries
    public Historical(){}

    public Historical(String country, int threeD, int highQuality, int totalRecords, LocalDateTime timestamp) {
        this.country = country;
        this.threeD = threeD;
        this.highQuality = highQuality;
        this.totalRecords = totalRecords;
        this.timestamp = timestamp;
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public int getThreeD() {
        return threeD;
    }

    public void setThreeD(int threeD) {
        this.threeD = threeD;
    }

    public int getHighQuality() {
        return highQuality;
    }

    public void setHighQuality(int highQuality) {
        this.highQuality = highQuality;
    }

    public int getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(int totalRecords) {
        this.totalRecords = totalRecords;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
}
