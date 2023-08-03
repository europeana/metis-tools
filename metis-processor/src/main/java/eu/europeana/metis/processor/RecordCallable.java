package eu.europeana.metis.processor;

import eu.europeana.corelib.edm.utils.EdmUtils;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.processor.utilities.ImageEnhancerUtil;
import eu.europeana.metis.processor.utilities.RdfUtil;
import eu.europeana.metis.schema.jibx.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Callable;

public class RecordCallable implements Callable<RDF> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final FullBeanImpl fullBean;
    private final ImageEnhancerUtil imageEnhancerUtil;
    private final RdfUtil rdfUtil = new RdfUtil();

    public RecordCallable(FullBeanImpl fullBean, ImageEnhancerUtil imageEnhancerUtil) {
        this.fullBean = fullBean;
        this.imageEnhancerUtil = imageEnhancerUtil;
    }

    @Override
    public RDF call() throws Exception {
        // TODO: 25/07/2023 Can we implement it with steps? 
        //Process RDF and exit
        RDF rdf = EdmUtils.toRDF(fullBean, true);
        if(rdfUtil.hasThumbnailsAndValidLicense(rdf)){
            LOGGER.info("RDF HAS thumbnails and valid license");
            LOGGER.info("Thread: {} - Processing RDF: {}", Thread.currentThread().getName(), rdf.getProvidedCHOList().get(0).getAbout());

            imageEnhancerUtil.processRecord(rdf);
        }
        return rdf;
    }

}
