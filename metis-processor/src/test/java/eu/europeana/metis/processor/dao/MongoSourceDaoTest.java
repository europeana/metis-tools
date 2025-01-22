package eu.europeana.metis.processor.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.processor.config.DataAccessConfigException;
import eu.europeana.metis.processor.dao.MongoSourceDaoTest.TestMongoConfiguration;
import eu.europeana.metis.processor.properties.general.ApplicationProperties;
import eu.europeana.metis.processor.properties.general.TruststoreProperties;
import eu.europeana.metis.processor.properties.mongo.MongoCoreProperties;
import eu.europeana.metis.processor.properties.mongo.MongoProcessorProperties;
import eu.europeana.metis.processor.properties.mongo.MongoSourceProperties;
import eu.europeana.metis.processor.properties.mongo.MongoTargetProperties;
import java.util.List;
import java.util.Optional;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ContextConfiguration(classes = {TestMongoConfiguration.class,
    ApplicationProperties.class, TruststoreProperties.class,
    MongoProcessorProperties.class, MongoTargetProperties.class,
    MongoSourceProperties.class, MongoCoreProperties.class,
})
@TestPropertySource(locations = "classpath:test.properties")
@SpringBootTest
class MongoSourceDaoTest {

  @Autowired
  private MongoSourceDao dao;

  private static Optional<FullBeanImpl> getLast(List<FullBeanImpl> fullBeanList, int pageSize) {
    Optional<FullBeanImpl> lastFullBean;
    if (!fullBeanList.isEmpty()) {
      if (fullBeanList.size() < pageSize) {
        lastFullBean = Optional.empty();
      } else {
        lastFullBean = Optional.ofNullable(fullBeanList.get(pageSize - 1));
      }
    } else {
      lastFullBean = Optional.empty();
    }
    return lastFullBean;
  }

  @TestConfiguration
  static class TestMongoConfiguration {

    @Bean
    public MongoSourceDao getMongoSourceDao(MongoSourceProperties mongoSourceProperties)
        throws DataAccessConfigException {
      return new MongoSourceDao(mongoSourceProperties);
    }
  }

  @Test
  void getTotalRecordsForDataset() {

    long total = dao.getTotalRecordsForDataset("1139");
    assertEquals(9689, total);

    total = dao.getTotalRecordsForDataset("1140");
    assertEquals(100, total);

    total = dao.getTotalRecordsForDataset("1106");
    assertEquals(8, total);

    total = dao.getTotalRecordsForDataset("1098");
    assertEquals(1000, total);
  }

  @Test
  void getNextPageOfRecords() {
    String datasetId = "1140";
    int pageSize = 20;
    List<FullBeanImpl> fullBeanList = dao.getNextPageOfRecords(datasetId, new ObjectId("000000000000000000000000"), pageSize);
    fullBeanList.forEach(fb -> System.out.println(fb.getId()));
    Optional<FullBeanImpl> lastFullBean = getLast(fullBeanList, pageSize);
    while (lastFullBean.isPresent()) {
      fullBeanList = dao.getNextPageOfRecords(datasetId, new ObjectId(lastFullBean.get().getId()), pageSize);
      fullBeanList.forEach(fb -> System.out.println(fb.getId()));
      lastFullBean = getLast(fullBeanList, pageSize);
    }
    assertTrue(true);
  }

  @Test
  void getTechnicalMetadataForHashCodes() {
  }
}
