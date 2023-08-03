package eu.europeana.metis.processor;

import eu.europeana.corelib.edm.utils.EdmUtils;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.image.enhancement.client.ImageEnhancerClient;
import eu.europeana.metis.image.enhancement.config.ImageEnhancerClientConfig;
import eu.europeana.metis.processor.image.enhancer.EnhancementProcessor;
import eu.europeana.metis.processor.utilities.RdfUtil;
import eu.europeana.metis.processor.utilities.S3Client;
import eu.europeana.metis.schema.jibx.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Callable;

public class RecordCallable implements Callable<RDF> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final FullBeanImpl fullBean;
    private final S3Client s3Client;
    private final RdfUtil rdfUtil = new RdfUtil();

    public RecordCallable(FullBeanImpl fullBean, S3Client s3Client) {
        this.fullBean = fullBean;
        this.s3Client = s3Client;
    }

//    000000456e84251e2d79533d8f7bb0db-LARGE


    @Override
    public RDF call() throws Exception {
        // TODO: 25/07/2023 Can we implement it with steps? 
        //Process RDF and exit
        RDF rdf = EdmUtils.toRDF(fullBean, true);
        if(rdfUtil.hasThumbnailsAndValidLicense(rdf)){
            LOGGER.info("RDF HAS thumbnails and valid license");
            LOGGER.info("Thread: {} - Processing RDF: {}", Thread.currentThread().getName(), rdf.getProvidedCHOList().get(0).getAbout());

            // Example prototype for processing records
            // ----------------------------------------
            ImageEnhancerClientConfig enhancerClientConfig = new ImageEnhancerClientConfig("http://localhost:5050",300,300);
            ImageEnhancerClient enhancerClient = new ImageEnhancerClient(enhancerClientConfig);
            EnhancementProcessor enhancementProcessor = new EnhancementProcessor(s3Client, enhancerClient);
            enhancementProcessor.processRecord(rdf);

            //Decide if we proceed
            //Extract thumbnail s3 file names
            //Get s3 thumbnail
            //Image enhance
            //Media thumbnail resize
            //Put s3 thumbnail(s)

            //Example pull and push
//            byte[] thumbnailObject = s3Client.getObject("000000456e84251e2d79533d8f7bb0db-LARGE");
//            final ObjectMetadata objectMetadata = new ObjectMetadata();
//            objectMetadata.setContentLength(thumbnailObject.length);
//            s3Client.putObject("000000456e84251e2d79533d8f7bb0db-LARGE", new ByteArrayInputStream(thumbnailObject), objectMetadata);
        }
//        else{
//            LOGGER.info("RDF HAS NO thumbnails or valid license");
//        }


//        LOGGER.info("Thread: {} - Processing RDF: {}", Thread.currentThread().getName(), rdf.getProvidedCHOList().get(0).getAbout());
        return rdf;
    }

}
