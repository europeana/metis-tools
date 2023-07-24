package eu.europeana.metis.processor.utilities;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;

import java.util.List;

public class DatasetPage {
    private final String datasetId;
    private final int page;
    private final List<FullBeanImpl> fullBeanList;

    public DatasetPage(String datasetId, int page, List<FullBeanImpl> fullBeanList) {
        this.datasetId = datasetId;
        this.page = page;
        this.fullBeanList = fullBeanList;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public int getPage() {
        return page;
    }

    public List<FullBeanImpl> getFullBeanList() {
        return fullBeanList;
    }

    public static class DatasetPageBuilder {

        private final String datasetId;
        private final int page;
        private List<FullBeanImpl> fullBeanList;

        public DatasetPageBuilder(String datasetId, int page) {
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

        public int getPage() {
            return page;
        }
    }
}
