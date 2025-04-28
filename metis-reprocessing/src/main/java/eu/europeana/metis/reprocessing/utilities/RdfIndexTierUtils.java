package eu.europeana.metis.reprocessing.utilities;

import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.RecordRelatedIndexingException;
import eu.europeana.indexing.tiers.model.Tier;
import eu.europeana.indexing.utils.RdfTier;
import eu.europeana.indexing.utils.RdfWrapper;
import eu.europeana.metis.schema.jibx.AboutType;
import eu.europeana.metis.schema.jibx.Aggregation;
import eu.europeana.metis.schema.jibx.EuropeanaAggregationType;
import eu.europeana.metis.schema.jibx.HasQualityAnnotation;
import eu.europeana.metis.schema.jibx.HasTarget;
import eu.europeana.metis.schema.jibx.QualityAnnotation;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.schema.jibx.ResourceType;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Rdf index tier utils.
 */
public class RdfIndexTierUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static QualityAnnotation getExistingAnnotation(RDF rdf, String target, Tier tier)
      throws RecordRelatedIndexingException {
    final RdfWrapper wrappedRecord = new RdfWrapper(rdf);
    final Stream<List<HasQualityAnnotation>> annotationsFromAggregations = wrappedRecord
        .getAggregations().stream().map(Aggregation::getHasQualityAnnotationList);
    final Stream<List<HasQualityAnnotation>> annotationsFromEuropeanaAggregation = wrappedRecord
        .getEuropeanaAggregation().stream().map(EuropeanaAggregationType::getHasQualityAnnotationList);
    final List<QualityAnnotation> resultCandidates = Stream
        .concat(annotationsFromAggregations, annotationsFromEuropeanaAggregation)
        .filter(Objects::nonNull).flatMap(List::stream).filter(Objects::nonNull)
        .map(HasQualityAnnotation::getQualityAnnotation).filter(Objects::nonNull)
        .filter(annotation -> annotationMatches(annotation, target, tier))
        .toList();
    if (resultCandidates.size() > 1) {
      throw new RecordRelatedIndexingException("Multiple annotations found for target '"
          + target + "' and type '" + tier.getClass() + "'.");
    } else if (!resultCandidates.isEmpty()
        && resultCandidates.getFirst().getHasTargetList().size() > 1) {
      throw new RecordRelatedIndexingException("Annotation found with multiple targets.");
    }
    return resultCandidates.isEmpty() ? null : resultCandidates.getFirst();
  }

  private static boolean annotationMatches(QualityAnnotation annotation, String target, Tier tier) {
    final boolean typeMatches = Optional.ofNullable(annotation.getHasBody())
                                        .map(ResourceType::getResource).orElse("").startsWith(RdfTier.getTierBaseUri(tier));
    return typeMatches && Optional.ofNullable(annotation.getHasTargetList())
                                  .stream().flatMap(List::stream).filter(Objects::nonNull).map(HasTarget::getResource)
                                  .filter(Objects::nonNull).anyMatch(target::equals);
  }

  private static EuropeanaTarget getEuropeanaTarget(RDF rdf) throws IndexingException {
    final EuropeanaAggregationType target = new RdfWrapper(rdf).getEuropeanaAggregation()
                                                               .orElseThrow(() -> new RecordRelatedIndexingException(
                                                                   "Cannot find suitable aggregator or provider aggregation in record."));
    return new EuropeanaTarget(target);
  }

  private static class EuropeanaTarget extends AbstractTierTarget<EuropeanaAggregationType> {

    /**
     * Instantiates a new Europeana target.
     *
     * @param target the target
     */
    public EuropeanaTarget(EuropeanaAggregationType target) {
      super(target);
    }

    @Override
    public List<HasQualityAnnotation> getHasQualityAnnotationList() {
      return super.getTarget().getHasQualityAnnotationList();
    }

    @Override
    public void setHasQualityAnnotationList(List<HasQualityAnnotation> hasQualityAnnotationList) {
      super.getTarget().setHasQualityAnnotationList(hasQualityAnnotationList);
    }
  }

  private abstract static class AbstractTierTarget<T extends AboutType> {

    private final T target;

    /**
     * Instantiates a new Abstract tier target.
     *
     * @param target the target
     */
    public AbstractTierTarget(T target) {
      this.target = target;
    }

    /**
     * Gets target.
     *
     * @return the target
     */
    public T getTarget() {
      return target;
    }

    /**
     * Gets about.
     *
     * @return the about
     */
    public String getAbout() {
      return target.getAbout();
    }

    /**
     * Gets has quality annotation list.
     *
     * @return the has quality annotation list
     */
    public abstract List<HasQualityAnnotation> getHasQualityAnnotationList();

    /**
     * Sets has quality annotation list.
     *
     * @param hasQualityAnnotationList the has quality annotation list
     */
    public abstract void setHasQualityAnnotationList(
        List<HasQualityAnnotation> hasQualityAnnotationList);
  }

  /**
   * MET-6359 Has content tier 1 boolean.
   *
   * @param rdf the rdf
   * @return the boolean
   * @throws IndexingException the indexing exception
   */
  static boolean hasContentTier(RDF rdf) throws IndexingException {
    AbstractTierTarget<?> target = getEuropeanaTarget(rdf);
    final Optional<QualityAnnotation> existingAnnotation = Optional.ofNullable(
        getExistingAnnotation(rdf, target.getAbout(), RdfTier.CONTENT_TIER_1.getTier()));

    if (existingAnnotation.isPresent()) {
      LOGGER.info("Has content tier 1: {}", target.getAbout());
      return true;
    } else {
      return false;
    }
  }
}
