package eu.europeana.metis.wiki;

import eu.europeana.enrichment.api.external.model.WikidataOrganization;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import eu.europeana.metis.zoho.exception.WikidataAccessException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 * Wikidata Dao class
 *
 * @author Srishti Singh (srishti.singh@europeana.eu)
 * @since 2021-07-06
 */
public class WikidataAccessDao {

    private Transformer transformer;

    private WikidataAccessDao(WikidataAccessDao.InputStreamCreator inputStreamSupplier) throws WikidataAccessException {
        try {
            InputStream inputStream = inputStreamSupplier.create();
            Exception var3 = null;

            try {
                this.init(inputStream);
            } catch (Exception var13) {
                var3 = var13;
                throw var13;
            } finally {
                if (inputStream != null) {
                    if (var3 != null) {
                        try {
                            inputStream.close();
                        } catch (Exception var12) {
                            var3.addSuppressed(var12);
                        }
                    } else {
                        inputStream.close();
                    }
                }

            }
        } catch (IOException var15) {
            throw new WikidataAccessException("Unexpected exception while reading the wikidata XSLT file.", var15);
        }
    }

    public WikidataAccessDao(File templateFile) throws WikidataAccessException {
        this(() -> {
            return Files.newInputStream(templateFile.toPath());
        });
    }

    public WikidataAccessDao(InputStream xslTemplate) throws WikidataAccessException {
        this(() -> {
            return xslTemplate;
        });
    }

    public WikidataAccessDao() throws WikidataAccessException {
        this(() -> {
            return WikidataAccessDao.class.getResourceAsStream("/wkd2org.xsl");
        });
    }

    public final void init(InputStream xslTemplate) throws WikidataAccessException {
        TransformerFactory transformerFactory = TransformerFactory.newInstance();

        try {
            Source xslt = new StreamSource(xslTemplate);
            this.transformer = transformerFactory.newTransformer(xslt);
            this.transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
            this.transformer.setParameter("deref", Boolean.TRUE);
            this.transformer.setParameter("address", Boolean.TRUE);
        } catch (TransformerConfigurationException var4) {
            throw new WikidataAccessException("Transformer could not be initialized.", var4);
        }
    }

    public StringBuilder getEntity(String uri) throws WikidataAccessException {
        StringBuilder res = new StringBuilder();
        StreamResult wikidataRes = new StreamResult(new StringBuilderWriter(res));
        String entityResponse = getEntityFromURL(uri);
        // transform the response
        if(entityResponse != null) {
            try (InputStream stream = new ByteArrayInputStream(entityResponse.getBytes(StandardCharsets.UTF_8))) {
                this.transformer.setParameter("rdf_about", uri);
                this.transformer.transform(new StreamSource(stream), wikidataRes);

            } catch (TransformerException | IOException e) {
                throw new WikidataAccessException("Error by transforming of Wikidata in RDF/XML.", e);
            }
        }
        return res;

    }

    /**
     * Method to get the RDF/xml response from wikidata using entityId
     * GET : <http://www.wikidata.org/entity/xyztesting>
     *
     * @param urlToRead
     * @return
     * @throws WikidataAccessException
     */
    public String getEntityFromURL(String urlToRead) throws WikidataAccessException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()){
            HttpGet request = new HttpGet(urlToRead);
            request.addHeader("Accept", "application/xml");
            CloseableHttpResponse response = httpClient.execute(request);
            try {
                if (response.getStatusLine().getStatusCode() != 200) {
                    return null;
                }
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                	//just for debugging purposes
//                	System.out.println(EntityUtils.toString(entity));
                	return EntityUtils.toString(entity);
                }
            } finally {
                response.close();
            }
        } catch (IOException e) {
            throw new WikidataAccessException("Error executing the request for uri "+urlToRead, e);
        }
        return null;

    }

    public WikidataOrganization parse(File xmlFile) throws JAXBException, IOException {
        String xml = FileUtils.readFileToString(xmlFile, StandardCharsets.UTF_8);
        return this.parse(xml);
    }

    public WikidataOrganization parse(InputStream xmlStream) throws JAXBException, IOException {
        StringWriter writer = new StringWriter();
        IOUtils.copy(xmlStream, writer, StandardCharsets.UTF_8);
        String wikidataXml = writer.toString();
        System.out.println(wikidataXml);
        return this.parse(wikidataXml);
    }

    public WikidataOrganization parse(String xml) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(new Class[]{WikidataOrganization.class});
        Unmarshaller unmarshaller = jc.createUnmarshaller();
        InputStream stream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
        return (WikidataOrganization)unmarshaller.unmarshal(stream);
    }

    public WikidataOrganization parseWikidataOrganization(File inputFile) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(new Class[]{WikidataOrganization.class});
        Unmarshaller unmarshaller = jc.createUnmarshaller();
        return (WikidataOrganization)unmarshaller.unmarshal(inputFile);
    }

    @FunctionalInterface
    private interface InputStreamCreator {
        InputStream create() throws IOException;
    }
}

