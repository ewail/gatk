package org.broadinstitute.hellbender.tools.spark.sv.discovery.inference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.SimpleSVType;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.SvDiscoverFromLocalAssemblyContigAlignmentsSpark;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.SvDiscoveryInputData;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment.AlignedContig;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment.AssemblyContigWithFineTunedAlignments;
import org.broadinstitute.hellbender.tools.spark.sv.utils.GATKSVVCFConstants;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVInterval;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.broadinstitute.hellbender.tools.spark.sv.discovery.inference.AssemblyContigAlignmentSignatureClassifier.RawTypes.Cpx;
import static org.broadinstitute.hellbender.tools.spark.sv.discovery.inference.SegmentedCpxVariantSimpleVariantExtractor.*;
import static org.broadinstitute.hellbender.tools.spark.sv.discovery.inference.SegmentedCpxVariantSimpleVariantExtractor.MultiSegmentsCpxVariantExtractor.getMissingAndPresentAndInvertedSegments;

/**
 * For extracting simple variants from input GATK-SV complex variants.
 *
 * Some explanation on several concepts:
 *
 * <p>
 *     Anchor ref base:
 *     anchor base is defined per-VCF spec (see 1.4.1#REF version 4.2), that is, for DEL and INS variants
 *     the reference base at the position pointed to by POS, basically:
 *     for DEL, the reference bases immediately following POS are deleted (up to and including the END base),
 *     for INS, the sequence annotated in INSSEQ are inserted immediately after POS.
 * </p>
 *
 * <p>
 *     "Fat" insertion:
 *     they exist because sometimes we have micro deletions surrounding the insertion breakpoint,
 *     so here the strategy is to report them as "fat", i.e. the anchor base and deleted bases are reported in REF;
 *     they are fat in the sense that compared to simple insertions where a single anchor ref base is necessary
 * </p>
 *
 * <p>
 *     It is also assumed that POS and END of the input complex {@link VariantContext} are the boundaries
 *     of the bases where REF and ALT allele share similarity, in other words,
 *     immediately after POS and before END is where the REF and ALT allele differ, and the two path merges at POS/END.
 * </p>
 */
class SimpleVariantExtractor implements Serializable {
    private static final long serialVersionUID = 1L;

    static final class ExtractedSimpleVariants {
        final List<VariantContext> reInterpretZeroOrOneSegmentCalls;
        final List<VariantContext> reInterpretMultiSegmentsCalls;

        private ExtractedSimpleVariants(final List<VariantContext> reInterpretZeroOrOneSegmentCalls,
                                        final List<VariantContext> reInterpretMultiSegmentsCalls) {
            this.reInterpretZeroOrOneSegmentCalls = reInterpretZeroOrOneSegmentCalls;
            this.reInterpretMultiSegmentsCalls = reInterpretMultiSegmentsCalls;
        }
    }

    /**
     * Exist for use by SV pipeline in memory.
     */
    public static ExtractedSimpleVariants extract(final JavaRDD<VariantContext> complexVariants,
                                                  final SvDiscoveryInputData svDiscoveryInputData,
                                                  final String nonCanonicalChromosomeNamesFile) {

        final Broadcast<ReferenceMultiSource> referenceBroadcast = svDiscoveryInputData.referenceBroadcast;
        final ZeroAndOneSegmentCpxVariantExtractor zeroAndOneSegmentCpxVariantExtractor = new ZeroAndOneSegmentCpxVariantExtractor();

        // still does an in-efficient 2-pass on the input RDD, but that was due to restriction from how multi-segment calls are to be re-interpreted
        final JavaRDD<VariantContext> reInterpretedZeroAndOneSegmentCalls =
                complexVariants
                    .flatMap(vc -> {
                        if (getAttributeAsStringList(vc, GATKSVVCFConstants.CPX_SV_REF_SEGMENTS).size() < 2) {
                            return zeroAndOneSegmentCpxVariantExtractor.extract(vc, referenceBroadcast.getValue()).iterator();
                        } else {
                            return Collections.emptyIterator();
                        }
                    });

        final JavaRDD<VariantContext> multiSegmentCalls =
                complexVariants.filter(vc -> getAttributeAsStringList(vc, GATKSVVCFConstants.CPX_SV_REF_SEGMENTS).size() > 1);
        final List<VariantContext> reInterpretMultiSegmentsCalls =
                reInterpretMultiSegmentsCalls(multiSegmentCalls, svDiscoveryInputData, nonCanonicalChromosomeNamesFile);

        return new ExtractedSimpleVariants(reInterpretedZeroAndOneSegmentCalls.collect(), reInterpretMultiSegmentsCalls);
    }

    //==================================================================================================================

    /**
     * Re-interpret CPX vcf records whose {@link GATKSVVCFConstants#CPX_SV_REF_SEGMENTS} has more than one entries,
     * aka "multi-segment" calls.
     *
     * @return the {@link SimpleSVType}-d variants extracted from the input {@code complexVariants}
     */
    private static List<VariantContext> reInterpretMultiSegmentsCalls(final JavaRDD<VariantContext> multiSegmentCalls,
                                                                      final SvDiscoveryInputData svDiscoveryInputData,
                                                                      final String nonCanonicalChromosomeNamesFile) {

        final List<VariantContext> oneSource = pairIterationWayOfReInterpretation(multiSegmentCalls, svDiscoveryInputData, nonCanonicalChromosomeNamesFile);

        final SegmentedCpxVariantSimpleVariantExtractor.MultiSegmentsCpxVariantExtractor multiSegmentsCpxVariantExtractor =
                new SegmentedCpxVariantSimpleVariantExtractor.MultiSegmentsCpxVariantExtractor();

        final Broadcast<ReferenceMultiSource> referenceBroadcast = svDiscoveryInputData.referenceBroadcast;
        final List<VariantContext> anotherSource = multiSegmentCalls
                .flatMap(vc -> multiSegmentsCpxVariantExtractor.extract(vc, referenceBroadcast.getValue()).iterator()).collect();
        oneSource.addAll(anotherSource);

        return removeDuplicates(oneSource);
    }

    /**
     * Exist basically to extract insertions, because
     * deletions and inversions are relatively easy to be extracted by
     * {@link org.broadinstitute.hellbender.tools.spark.sv.discovery.inference.SegmentedCpxVariantSimpleVariantExtractor.MultiSegmentsCpxVariantExtractor}
     */
    @VisibleForTesting
    static List<VariantContext> pairIterationWayOfReInterpretation(final JavaRDD<VariantContext> multiSegmentCalls,
                                                                   final SvDiscoveryInputData svDiscoveryInputData,
                                                                   final String nonCanonicalChromosomeNamesFile) {

        // not a multimap because we know a single assembly contig can only generate a single complex variant
        final Map<String, VariantContext> contigNameToCpxVariant =
                multiSegmentCalls
                        .flatMapToPair(vc ->
                                getAttributeAsStringList(vc, GATKSVVCFConstants.CONTIG_NAMES).stream()
                                        .map(name -> new Tuple2<>(name, vc))
                                        .iterator()
                        )
                        .collectAsMap();

        // resend the relevant contigs through the pair-iteration-ed path
        final Set<String> relevantContigs = new HashSet<>( contigNameToCpxVariant.keySet() );
        JavaRDD<GATKRead> assemblyRawAlignments = svDiscoveryInputData.assemblyRawAlignments;
        final JavaRDD<GATKRead> relevantAlignments = assemblyRawAlignments.filter(read -> relevantContigs.contains(read.getName()));
        final SvDiscoveryInputData filteredData =
                new SvDiscoveryInputData(svDiscoveryInputData.sampleId, svDiscoveryInputData.discoverStageArgs,
                        svDiscoveryInputData.outputPath, svDiscoveryInputData.metadata, svDiscoveryInputData.assembledIntervals,
                        svDiscoveryInputData.evidenceTargetLinks, relevantAlignments, svDiscoveryInputData.toolLogger,
                        svDiscoveryInputData.referenceBroadcast, svDiscoveryInputData.referenceSequenceDictionaryBroadcast,
                        svDiscoveryInputData.headerBroadcast, svDiscoveryInputData.cnvCallsBroadcast);
        final JavaRDD<AlignedContig> analysisReadyContigs =
                SvDiscoverFromLocalAssemblyContigAlignmentsSpark.preprocess(filteredData, nonCanonicalChromosomeNamesFile, false)
                        .get(Cpx)
                        .map(AssemblyContigWithFineTunedAlignments::getSourceContig);
        @SuppressWarnings("deprecation")
        final List<VariantContext> reInterpreted =
                org.broadinstitute.hellbender.tools.spark.sv.discovery
                        .DiscoverVariantsFromContigAlignmentsSAMSpark
                        .discoverVariantsFromChimeras(filteredData, analysisReadyContigs);

        // filter for consistency, then add annotation for signalling that they were extracted from CPX variants
        final  List<VariantContext> consistentOnes = new ArrayList<>(reInterpreted.size());
        for (final VariantContext simpleVC : reInterpreted) {
            final List<String> consistentComplexVariantIDs =
                    getAttributeAsStringList(simpleVC, GATKSVVCFConstants.CONTIG_NAMES).stream()
                            .map(contigNameToCpxVariant::get)
                            .filter(cpx -> isConsistentWithCPX(simpleVC, cpx))
                            .map(VariantContext::getID)
                            .collect(Collectors.toList());
            if ( ! consistentComplexVariantIDs.isEmpty()) {

                final VariantContext updatedSimpleVC = new VariantContextBuilder(simpleVC)
                        .attribute(EVENT_KEY,
                                String.join(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR, consistentComplexVariantIDs))
                        .id(simpleVC.getID() + GATKSVVCFConstants.INTERVAL_VARIANT_ID_FIELD_SEPARATOR + CPX_DERIVED_POSTFIX_STRING)
                        .make();
                consistentOnes.add(updatedSimpleVC);
            }
        }
        return consistentOnes;
    }

    // TODO: 3/26/18 here we check consistency only for DEL and INV calls
    /**
     * @param simple   simple variant derived from pair-iteration logic that is to be checked
     * @param complex  CPX variant induced by the same contig that induced the simple variant
     */
    @VisibleForTesting
    static boolean isConsistentWithCPX(final VariantContext simple, final VariantContext complex) {

        final String typeString = simple.getAttributeAsString(GATKSVVCFConstants.SVTYPE, "");

        if ( typeString.equals(SimpleSVType.TYPES.DEL.name()) || typeString.equals(SimpleSVType.TYPES.INV.name()) ) {
            final List<SimpleInterval> refSegments = getAttributeAsStringList(complex, GATKSVVCFConstants.CPX_SV_REF_SEGMENTS)
                    .stream().map(SimpleInterval::new).collect(Collectors.toList());
            final List<String> altArrangement = getAttributeAsStringList(complex, GATKSVVCFConstants.CPX_EVENT_ALT_ARRANGEMENTS);

            final Tuple3<Set<SimpleInterval>, Set<Integer>, List<Integer>> missingAndPresentAndInvertedSegments =
                    getMissingAndPresentAndInvertedSegments(refSegments, altArrangement);
            final Set<SimpleInterval> missingSegments = missingAndPresentAndInvertedSegments._1();
            final Set<Integer> presentSegments = missingAndPresentAndInvertedSegments._2();
            final List<Integer> invertedSegments = missingAndPresentAndInvertedSegments._3();

            if ( typeString.equals(SimpleSVType.TYPES.INV.name()) )
                return inversionConsistencyCheck(simple, refSegments, presentSegments, invertedSegments);
            else
                return deletionConsistencyCheck(simple, missingSegments);
        } else
            return true;
    }

    private static boolean inversionConsistencyCheck(final VariantContext simple, final List<SimpleInterval> refSegments,
                                                     final Set<Integer> presentSegments, final List<Integer> invertedSegments) {
        if (invertedSegments.isEmpty()) return false;

        final SimpleInterval invertedRange = new SimpleInterval(simple.getContig(), simple.getStart(), simple.getEnd());
        // dummy number for chr to be used in constructing SVInterval, since 2 input AI's both map to the same chr by this point
        final int dummyChr = -1;
        final SVInterval intervalOne = new SVInterval(dummyChr, invertedRange.getStart() - 1, invertedRange.getEnd());
        for (int i = 0; i < refSegments.size(); ++i) {
            final SimpleInterval segment = refSegments.get(i);
            final SVInterval intervalTwo = new SVInterval(dummyChr, segment.getStart() - 1, segment.getEnd());
            // allow 1-base fuzziness from either end
            if( 2 >= Math.abs( Math.min(segment.size(), invertedRange.size()) - intervalTwo.overlapLen(intervalOne) ) ){ // this also gets rid of a case where a large deletion is interpreted but is actually a dispersed duplication (the stringent kills it)
                return (invertedSegments.contains(i)) && (!presentSegments.contains(i)); // inverted range appears in inverted segments, but absent in alt description without the "-" sign (if present, treat as insertion)
            }
        }
        return false;
    }

    private static boolean deletionConsistencyCheck(final VariantContext simple, final Set<SimpleInterval> missingSegments) {
        if (missingSegments.isEmpty()) return false;

        final SimpleInterval deletedRange = new SimpleInterval(simple.getContig(), simple.getStart(), simple.getEnd());
        // dummy number for chr to be used in constructing SVInterval, since 2 input AI's both map to the same chr by this point
        final int dummyChr = -1;
        final SVInterval intervalOne = new SVInterval(dummyChr, deletedRange.getStart() - 1, deletedRange.getEnd());

        for (final SimpleInterval missing : missingSegments) {
            final SVInterval intervalTwo = new SVInterval(dummyChr, missing.getStart() - 1, missing.getEnd());
            // allow 1-base fuzziness from either end
            if ( Math.abs(missing.size() - deletedRange.size()) > 2 )
                return false;
            if( 2 >= Math.abs( Math.min(missing.size(), deletedRange.size()) - intervalTwo.overlapLen(intervalOne) ) ){
                return true;
            }
        }
        return false;
    }

    //==================================================================================================================

    /**
     * Exist because the two ways to re-interpret simple variants via
     * {@link #pairIterationWayOfReInterpretation(JavaRDD, SvDiscoveryInputData, String)}
     * and via
     * {@link MultiSegmentsCpxVariantExtractor}
     * could give essentially the same variants.
     */
    @VisibleForTesting
    static List<VariantContext> removeDuplicates(final List<VariantContext> toUniquify) {
        // separate by types {DEL, INS/DUP, INV} and compare with same type
        final List<VariantContext> deletions = new ArrayList<>();
        final List<VariantContext> insertions = new ArrayList<>();
        final List<VariantContext> inversions = new ArrayList<>();
        final List<VariantContext> duplications = new ArrayList<>();

        toUniquify.forEach(vc -> {
            final String type = vc.getAttributeAsString(GATKSVVCFConstants.SVTYPE, "");
            if(type.equals(SimpleSVType.TYPES.DEL.name())) {
                deletions.add(vc);
            } else if (type.equals(SimpleSVType.TYPES.INV.name())) {
                inversions.add(vc);
            } else if (type.equals(SimpleSVType.TYPES.INS.name())) {
                insertions.add(vc);
            } else if (type.equals(SimpleSVType.TYPES.DUP.name())) {
                duplications.add(vc);
            }
        });

        final List<VariantContext> result = new ArrayList<>(deletions.size() + inversions.size() + insertions.size() + duplications.size());
        result.addAll(mergeSameVariants(deletions));
        result.addAll(mergeSameVariants(inversions));
        result.addAll(mergeSameVariants(insertions));
        result.addAll(mergeSameVariants(duplications));

        return result;
    }

    /**
     * Exist for equals() and hashCode()
     */
    private static final class AnnotatedInterval {

        private static final List<String> NO_SUCH_ATTRIBUTE = Collections.emptyList();

        private final VariantContext sourceVC; // NOTE: omitted in equals() and hashCode() on purpose

        final SimpleInterval interval;
        final String id;
        final String type;
        final int svlen;
        final List<Allele> alleles;

        private AnnotatedInterval(final VariantContext vc) {
            sourceVC = vc;
            interval = new SimpleInterval( vc.getContig(), vc.getStart(), vc.getEnd());
            id = vc.getID();
            type = vc.getAttributeAsString(GATKSVVCFConstants.SVTYPE, "");
            svlen = vc.getAttributeAsInt(GATKSVVCFConstants.SVLEN, 0);
            alleles = vc.getAlleles();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final AnnotatedInterval interval1 = (AnnotatedInterval) o;

            if (svlen != interval1.svlen) return false;
            if (!interval.equals(interval1.interval)) return false;
            if (!id.equals(interval1.id)) return false;
            if (!type.equals(interval1.type)) return false;
            return alleles.equals(interval1.alleles);
        }

        @Override
        public int hashCode() {
            int result = interval.hashCode();
            result = 31 * result + id.hashCode();
            result = 31 * result + type.hashCode();
            result = 31 * result + svlen;
            result = 31 * result + alleles.hashCode();
            return result;
        }
    }

    @VisibleForTesting
    static List<VariantContext> mergeSameVariants(final List<VariantContext> vcs) {

        final Map<AnnotatedInterval, Tuple2<Set<String>, Set<String>>> rangeToAnnotations = new HashMap<>();

        for(final VariantContext vc : vcs) {
            final AnnotatedInterval interval = new AnnotatedInterval(vc);
            Tuple2<Set<String>, Set<String>> annotation = rangeToAnnotations.get(interval);
            if (annotation == null) {
                rangeToAnnotations.put(interval,
                        new Tuple2<>(
                                Sets.newHashSet(vc.getAttributeAsString(EVENT_KEY, "")),
                                new HashSet<>( getAttributeAsStringList(vc, GATKSVVCFConstants.CONTIG_NAMES)))
                );
            } else {
                annotation._1.add(vc.getAttributeAsString(EVENT_KEY, ""));
                annotation._2.addAll(getAttributeAsStringList(vc, GATKSVVCFConstants.CONTIG_NAMES));
            }
        }

        final List<VariantContext> result = new ArrayList<>();
        for (final Map.Entry<AnnotatedInterval, Tuple2<Set<String>, Set<String>>> entry: rangeToAnnotations.entrySet()) {
            final AnnotatedInterval interval = entry.getKey();
            final VariantContextBuilder variant = new VariantContextBuilder(interval.sourceVC)
                    .rmAttribute(EVENT_KEY)
                    .attribute(EVENT_KEY, String.join(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR, entry.getValue()._1))
                    .rmAttribute(GATKSVVCFConstants.CONTIG_NAMES)
                    .attribute(GATKSVVCFConstants.CONTIG_NAMES, String.join(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR, entry.getValue()._2));
            result.add( variant.make());
        }
        return result;
    }
}
