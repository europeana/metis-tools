package eu.europeana.metis_tools.inc_harvest.data;

import static eu.europeana.metis.core.common.DaoFieldNames.ID;

import dev.morphia.query.FindOptions;
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

  public Stream<String> getAllRecordIds(String datasetId) {
    final Pattern pattern = Pattern.compile("^/" + Pattern.quote(datasetId) + "/");
    return ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(() -> {
      final FindOptions findOptions = new FindOptions()
              .projection().exclude(ID.getFieldName())
              .projection().include(ABOUT_FIELD);
      final Iterator<FullBeanImpl> resultIterator = recordDao.getDatastore()
              .find(FullBeanImpl.class)
              .filter(Filters.regex(ABOUT_FIELD).pattern(pattern))
              .iterator(findOptions);
      return StreamSupport.stream(Spliterators.spliteratorUnknownSize(resultIterator, 0), false)
              .map(FullBeanImpl::getAbout);
    });
  }
}
