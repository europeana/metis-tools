package eu.europeana.metis.processor.utilities;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import java.util.List;
import java.util.Objects;
import org.bson.types.ObjectId;

public class DatasetPage {

  private final String datasetId;
  private final ObjectId pageId;
  private final List<FullBeanImpl> fullBeanList;

  public DatasetPage(String datasetId, ObjectId pageId, List<FullBeanImpl> fullBeanList) {
    this.datasetId = datasetId;
    this.pageId = pageId;
    this.fullBeanList = fullBeanList;
  }

  @Override
  public String toString() {
    return "DatasetPage{" +
        "datasetId='" + datasetId + '\'' +
        ", pageId=" + pageId +
        ", fullBeanList=" + fullBeanList +
        '}';
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DatasetPage)) {
      return false;
    }

    DatasetPage that = (DatasetPage) o;
    return Objects.hashCode(pageId) == Objects.hashCode(that.pageId) && datasetId.equals(that.datasetId) && fullBeanList.equals(
        that.fullBeanList);
  }

  @Override
  public int hashCode() {
    int result = datasetId.hashCode();
    result = 31 * result + Objects.hashCode(pageId);
    result = 31 * result + fullBeanList.hashCode();
    return result;
  }

  public String getDatasetId() {
    return datasetId;
  }

  public ObjectId getPageId() {
    return pageId;
  }

  public List<FullBeanImpl> getFullBeanList() {
    return fullBeanList;
  }

  public static class DatasetPageBuilder {

    private final String datasetId;
    private final ObjectId page;
    private List<FullBeanImpl> fullBeanList;

    public DatasetPageBuilder(String datasetId, ObjectId page) {
      this.datasetId = datasetId;
      this.page = page;
    }

    public DatasetPageBuilder setFullBeanList(List<FullBeanImpl> fullBeanList) {
      this.fullBeanList = fullBeanList;
      return this;
    }

    public DatasetPage build() {
      return new DatasetPage(this.datasetId, this.page, this.fullBeanList);
    }

    public String getDatasetId() {
      return datasetId;
    }

    public ObjectId getPage() {
      return page;
    }
  }
}
