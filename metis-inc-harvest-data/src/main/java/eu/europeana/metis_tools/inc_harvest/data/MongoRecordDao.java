package eu.europeana.metis_tools.inc_harvest.data;

import static eu.europeana.metis.core.common.DaoFieldNames.ID;

import dev.morphia.aggregation.experimental.Aggregation;
import dev.morphia.aggregation.experimental.expressions.Expressions;
import dev.morphia.aggregation.experimental.stages.Projection;
import dev.morphia.annotations.Entity;
import dev.morphia.query.experimental.filters.Filters;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.mongo.dao.RecordDao;
import eu.europeana.metis.network.ExternalRequestUtil;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MongoRecordDao {

  private static final String ABOUT_FIELD = "about";

  private final RecordDao recordDao;

  public MongoRecordDao(MongoClientProvider<IllegalArgumentException> mongoClientProvider,
          String mongoCoreDb) {
    recordDao = new RecordDao(mongoClientProvider.createMongoClient(), mongoCoreDb);
  }

  public Stream<String> getAllRecordIds(String datasetId){
    final Pattern pattern = Pattern.compile("^/" + Pattern.quote(datasetId) + "/");
    return ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(() -> {

      // Create aggregation pipeline finding all datasets.
      final Aggregation<FullBeanImpl> pipeline = recordDao.getDatastore()
              .aggregate(FullBeanImpl.class);

      // Filter on dataset ID
      pipeline.match(Filters.regex(ABOUT_FIELD).pattern(pattern));

      // The field name should be the field name in RecordIdWrapper.
      final String recordIdField = "recordId";

      // Project the dataset ID to the right field name.
      pipeline.project(Projection.project().exclude(ID.getFieldName())
              .include(recordIdField, Expressions.field(ABOUT_FIELD)));

      // Perform the aggregation and add the IDs in the result set.
      final Iterator<RecordIdWrapper> resultIterator = pipeline.execute(RecordIdWrapper.class);
      return StreamSupport.stream(Spliterators.spliteratorUnknownSize(resultIterator, 0), false)
              .map(RecordIdWrapper::getRecordId);
    });
  }

  @Entity
  private static class RecordIdWrapper {

    // Name depends on the mongo aggregations in which it is used.
    private String recordId;

    public String getRecordId() {
      return recordId;
    }
  }
}
