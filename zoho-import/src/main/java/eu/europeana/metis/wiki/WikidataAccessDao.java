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

import eu.europeana.metis.utils.Constants;
import eu.europeana.metis.zoho.exception.WikidataAccessException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CharSequenceReader;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.jena.rdf.model.*;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDFS;

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
        this.translate(uri, wikidataRes);
        return res;
    }

    public WikidataOrganization parse(File xmlFile) throws JAXBException, IOException {
        String xml = FileUtils.readFileToString(xmlFile, StandardCharsets.UTF_8);
        return this.parse(xml);
    }

    public WikidataOrganization parse(InputStream xmlStream) throws JAXBException, IOException {
        StringWriter writer = new StringWriter();
        IOUtils.copy(xmlStream, writer, StandardCharsets.UTF_8);
        String wikidataXml = writer.toString();
        return this.parse(wikidataXml);
    }

    public WikidataOrganization parse(String xml) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(new Class[]{WikidataOrganization.class});
        Unmarshaller unmarshaller = jc.createUnmarshaller();
        InputStream stream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
        return (WikidataOrganization)unmarshaller.unmarshal(stream);
    }

    private Resource getModelFromSPARQL(String uri) throws WikidataAccessException {
        Resource resource = this.fetchFromSPARQL(uri);
        if (!this.isDuplicate(resource)) {
            return resource;
        } else {
            StmtIterator iter = resource.listProperties(OWL.sameAs);

            try {
                while(iter.hasNext()) {
                    String sameAs = ((Statement)iter.next()).getResource().getURI();
                    Resource r2 = this.fetchFromSPARQL(sameAs);
                    if (!this.isDuplicate(r2)) {
                        resource = r2;
                        break;
                    }
                }
            } finally {
                iter.close();
            }

            return resource;
        }
    }

    private boolean isDuplicate(Resource resource) {
        return resource != null && resource.hasProperty(OWL.sameAs) && !resource.hasProperty(RDFS.label);
    }

    private Resource fetchFromSPARQL(String uri) throws WikidataAccessException {
        String sDescribe = "DESCRIBE <" + uri + ">";
        Model m = ModelFactory.createDefaultModel();
        Resource var5;
        try (QueryEngineHTTP endpoint = new QueryEngineHTTP(Constants.SPARQL, sDescribe)) {
            var5 = endpoint.execDescribe(m).getResource(uri);
        } catch (Exception var9) {
            throw new WikidataAccessException("Cannot access wikidata resource: " + uri, var9);
        }
        return var5;
    }

    private synchronized void transform(Resource resource, StreamResult res) throws WikidataAccessException {
        this.transformer.setParameter(Constants.RDF_ABOUT, resource.getURI());
        StringBuilder sb = new StringBuilder(Constants.SIZE);
        try (StringBuilderWriter sbw = new StringBuilderWriter(sb)){
            Model model = ModelFactory.createDefaultModel();
            RDFWriter writer = model.getWriter("RDF/XML");
            writer.setProperty("tab", "0");
            writer.setProperty("allowBadURIs", "true");
            writer.setProperty("relativeURIs", "");
            writer.write(model, sbw, "RDF/XML");
            this.transformer.transform(new StreamSource(new CharSequenceReader(sb)), res);
        } catch (TransformerException var26) {
            throw new WikidataAccessException("Error by transforming of Wikidata in RDF/XML.", var26);
        } finally {
            sb.setLength(0);
        }
    }

    public WikidataOrganization parseWikidataOrganization(File inputFile) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(new Class[]{WikidataOrganization.class});
        Unmarshaller unmarshaller = jc.createUnmarshaller();
        return (WikidataOrganization)unmarshaller.unmarshal(inputFile);
    }

    public void translate(String uri, StreamResult res) throws WikidataAccessException {
        Resource wikidataResource = this.getModelFromSPARQL(uri);
        if (wikidataResource != null && wikidataResource.getURI() != null) {
            this.transform(wikidataResource, res);
        } else {
            throw new WikidataAccessException("Cannot access wikidata resource: " + uri, null);
        }
    }

    @FunctionalInterface
    private interface InputStreamCreator {
        InputStream create() throws IOException;
    }
}

