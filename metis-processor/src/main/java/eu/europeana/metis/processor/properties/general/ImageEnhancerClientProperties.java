package eu.europeana.metis.processor.properties.general;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * The type Image enhancer client properties.
 */
@Component
public class ImageEnhancerClientProperties {

    @Value("${image.enhancer.endpoint}")
    private String imageEnhancerEndpoint;
    @Value("${image.enhancer.connect.timeout}")
    private int imageEnhancerConnectTimeout;
    @Value("${image.enhancer.read.timeout}")
    private int imageEnhancerReadTimeout;
    @Value("${image.enhancer.report.path}")
    private String imageEnhancerReportPath;
    @Value("${image.enhancer.script.path}")
    private String imageEnhancerScriptPath;

    /**
     * Gets image enhancer endpoint.
     *
     * @return the image enhancer endpoint
     */
    public String getImageEnhancerEndpoint() {
        return imageEnhancerEndpoint;
    }

    /**
     * Gets image enhancer connect timeout.
     *
     * @return the image enhancer connect timeout
     */
    public int getImageEnhancerConnectTimeout() {
        return imageEnhancerConnectTimeout;
    }

    /**
     * Gets image enhancer read timeout.
     *
     * @return the image enhancer read timeout
     */
    public int getImageEnhancerReadTimeout() {
        return imageEnhancerReadTimeout;
    }

    /**
     * Gets image enhancer report path.
     *
     * @return the image enhancer report path
     */
    public String getImageEnhancerReportPath() {
        return imageEnhancerReportPath;
    }

    /**
     * Gets image enhancer script path.
     *
     * @return the image enhancer script path
     */
    public String getImageEnhancerScriptPath() {
        return imageEnhancerScriptPath;
    }
}
