package org.broadinstitute.hellbender.tools.spark.sv.discovery.inference;

import com.google.common.annotations.VisibleForTesting;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.math.NumberUtils;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.SimpleSVType;
import org.broadinstitute.hellbender.tools.spark.sv.utils.GATKSVVCFConstants;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import scala.Tuple3;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

abstract class SegmentedCpxVariantSimpleVariantExtractor implements Serializable {
    private static final long serialVersionUID = 1L;

    // TODO: 5/2/18 for use in output VCF to link to original CPX variant, to be moved to GATKSVVCFConstants
    static String EVENT_KEY = "CPX_EVENT";
    static int EVENT_SIZE_THRESHOLD = 49;
    static final String CPX_DERIVED_POSTFIX_STRING = "CPX_DERIVED";
    private static String makeID(final String typeName, final String chr, final int start, final int stop) {
        return typeName + GATKSVVCFConstants.INTERVAL_VARIANT_ID_FIELD_SEPARATOR
                + chr + GATKSVVCFConstants.INTERVAL_VARIANT_ID_FIELD_SEPARATOR
                + start + GATKSVVCFConstants.INTERVAL_VARIANT_ID_FIELD_SEPARATOR
                + stop + GATKSVVCFConstants.INTERVAL_VARIANT_ID_FIELD_SEPARATOR +
                CPX_DERIVED_POSTFIX_STRING;
    }

    // TODO: 5/2/18 move to a utility class
    /**
     * this exist because for whatever reason,
     * VC.getAttributeAsStringList() sometimes returns a giant single string, while using
     * VC.getAttributeAsString() gives back an array.....
     */
    static List<String> getAttributeAsStringList(final VariantContext vc, final String attributeKey) {
        return vc.getAttributeAsStringList(attributeKey, "").stream()
                .flatMap(s -> {
                    if ( s.contains(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR) ) {
                        final String[] split = s.split(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR);
                        return Arrays.stream(split);
                    } else {
                        return Stream.of(s);
                    }
                })
                .collect(Collectors.toList());
    }
    static SimpleInterval makeOneBpInterval(final String chr, final int pos) {
        return new SimpleInterval(chr, pos, pos);
    }


    abstract List<VariantContext> extract(final VariantContext complexVC, final ReferenceMultiSource reference);

    //==================================================================================================================

    static final class ZeroAndOneSegmentCpxVariantExtractor extends SegmentedCpxVariantSimpleVariantExtractor {
        private static final long serialVersionUID = 1L;

        /**
         * Depending on how the ref segment is present in alt arrangement (if at all), logic as follows (order is important):
         * <ul>
         *     <li>
         *         if ref segment appear inverted and large enough
         *         <ul>
         *             <li> INV call is warranted </li>
         *             <li> INS call(s) before and after the INV, if inserted sequence long enough </li>
         *         </ul>
         *     </li>
         *
         *     <li>
         *         otherwise if ref segment is present as-is, i.e. no deletion call can be made,
         *         make insertion calls when possible
         *     </li>
         *     <li>
         *         otherwise
         *         <ul>
         *             <li> if the segment is large enough, make a DEL call, and insertion calls when possible </li>
         *             <li> otherwise a single fat INS call</li>
         *         </ul>
         *     </li>
         * </ul>
         *
         * <p>
         *     Note that the above logic has a bias towards getting INV calls, because
         *     when the (large enough) reference segment appears both as-is and inverted,
         *     the above logic will emit at least an INV call,
         *     whereas the (inverted) duplication(s) could also be reported as an DUP call as well, but...
         * </p>
         */
        @Override
        List<VariantContext> extract(final VariantContext complexVC, final ReferenceMultiSource reference) {

            final List<String> segments = getAttributeAsStringList(complexVC, GATKSVVCFConstants.CPX_SV_REF_SEGMENTS);
            if (segments.isEmpty()) return whenZeroSegments(complexVC, reference);

            final SimpleInterval refSegment = new SimpleInterval(segments.get(0));
            final List<String> altArrangement = getAttributeAsStringList(complexVC, GATKSVVCFConstants.CPX_EVENT_ALT_ARRANGEMENTS);
            final int altSeqLength = complexVC.getAttributeAsString(GATKSVVCFConstants.SEQ_ALT_HAPLOTYPE, "").length();

            final List<VariantContextBuilder> result = new ArrayList<>();

            final int asIsAppearanceIdx = altArrangement.indexOf("1");
            final int invertedAppearanceIdx = altArrangement.indexOf("-1");
            if (invertedAppearanceIdx != -1 && refSegment.size() > EVENT_SIZE_THRESHOLD) { // inversion call
                whenInversionIsWarranted(refSegment, invertedAppearanceIdx, altArrangement, reference, result);
            } else if (asIsAppearanceIdx != -1) { // no inverted appearance or appear inverted but not large enough, and in the mean time appear as-is, so no deletion
                whenNoDeletionIsAllowed(refSegment, asIsAppearanceIdx, altArrangement, altSeqLength, reference, result);
            } else { // no as-is appearance && (inverted appearance might present not not large enough)
                whenNoInvAndNoAsIsAppearance(refSegment, altSeqLength, reference, result);
            }

            final String sourceID = complexVC.getID();
            final List<String> evidenceContigs = getAttributeAsStringList(complexVC, GATKSVVCFConstants.CONTIG_NAMES);
            return result.stream()
                    .map(vc -> vc.attribute(EVENT_KEY, sourceID).attribute(GATKSVVCFConstants.CONTIG_NAMES, evidenceContigs).make())
                    .collect(Collectors.toList());
        }

        private List<VariantContext> whenZeroSegments(final VariantContext complexVC, final ReferenceMultiSource reference) {
            final Allele anchorBaseRefAllele = getAnchorBaseRefAllele(complexVC.getContig(), complexVC.getStart(), reference);
            final int altSeqLength = complexVC.getAttributeAsString(GATKSVVCFConstants.SEQ_ALT_HAPLOTYPE, "").length() - 1;
            final VariantContext insertion = makeInsertion(complexVC.getContig(), complexVC.getStart(), complexVC.getStart(), altSeqLength, anchorBaseRefAllele).make();
            return Collections.singletonList(insertion);
        }

        private static void whenInversionIsWarranted(final SimpleInterval refSegment, final int invertedAppearanceIdx,
                                                     final List<String> altArrangement, final ReferenceMultiSource reference,
                                                     final List<VariantContextBuilder> result) {

            final Allele anchorBaseRefAllele = getAnchorBaseRefAllele(refSegment.getContig(), refSegment.getStart(), reference);
            result.add( makeInversion(refSegment, anchorBaseRefAllele) );

            // further check if alt seq length is long enough to trigger an insertion as well,
            // but guard against case smallIns1 + INV + smallIns2, in theory one could annotate the inversion
            // with micro-insertions if that's the case, but we try to have minimal annotations here
            final Allele anchorBaseRefAlleleFront = getAnchorBaseRefAllele(refSegment.getContig(), refSegment.getStart() - 1, reference);
            final Allele anchorBaseRefAlleleRear  = getAnchorBaseRefAllele(refSegment.getContig(), refSegment.getEnd(), reference);
            extractFrontAndRearInsertions(refSegment, invertedAppearanceIdx, altArrangement,
                    anchorBaseRefAlleleFront, anchorBaseRefAlleleRear, result);
        }

        private static void whenNoDeletionIsAllowed(final SimpleInterval refSegment, final int asIsAppearanceIdx,
                                                    final List<String> altArrangement, final int altSeqLength,
                                                    final ReferenceMultiSource reference, final List<VariantContextBuilder> result) {
            final int segmentSize = refSegment.size();
            if (altSeqLength - segmentSize > EVENT_SIZE_THRESHOLD ) { // long enough net gain to trigger insertion calls
                // distinguish between cases {"1", ....}, {....., "1"}, and {....., "1", ....} to know where to place the insertion
                final Allele anchorBaseRefAlleleFront = getAnchorBaseRefAllele(refSegment.getContig(), refSegment.getStart() - 1, reference);
                final Allele anchorBaseRefAlleleRear = getAnchorBaseRefAllele(refSegment.getContig(), refSegment.getEnd(), reference);
                if ( altArrangement.get(altArrangement.size() - 1).equals("1") ) { // {....., "1"} -> front insertion
                    final VariantContextBuilder frontIns =
                            SegmentedCpxVariantSimpleVariantExtractor.makeInsertion(refSegment.getContig(),
                                    refSegment.getStart() - 1, refSegment.getStart() - 1,
                                    altSeqLength - segmentSize, anchorBaseRefAlleleFront);
                    result.add(frontIns);
                } else if ( altArrangement.get(0).equals("1") ) { // {"1", ....} -> rear insertion
                    final VariantContextBuilder rearIns =
                            SegmentedCpxVariantSimpleVariantExtractor.makeInsertion(refSegment.getContig(),
                                    refSegment.getEnd(), refSegment.getEnd(),
                                    altSeqLength - segmentSize, anchorBaseRefAlleleFront);
                    result.add(rearIns);
                } else { // {....., "1", ....} -> collect new insertion length before and after
                    extractFrontAndRearInsertions(refSegment, asIsAppearanceIdx, altArrangement,
                            anchorBaseRefAlleleFront, anchorBaseRefAlleleRear, result);
                }
            }
        }

        private static void whenNoInvAndNoAsIsAppearance(final SimpleInterval refSegment, final int altSeqLength,
                                                         final ReferenceMultiSource reference, final List<VariantContextBuilder> result) {
            if ( refSegment.size() > EVENT_SIZE_THRESHOLD ) { // a deletion call must be present

                final Allele anchorBaseRefAlleleFront = getAnchorBaseRefAllele(refSegment.getContig(), refSegment.getStart(), reference);

                // need left shift because the segment boundaries are shared by REF and ALT
                result.add( makeDeletion(new SimpleInterval(refSegment.getContig(), refSegment.getStart(), refSegment.getEnd() - 1),
                                         anchorBaseRefAlleleFront) );

                // if the replacing sequence is long enough to trigger an insertion as well
                if (altSeqLength - 2 > EVENT_SIZE_THRESHOLD) {
                    result.add(makeInsertion(refSegment.getContig(), refSegment.getStart(), refSegment.getStart(), altSeqLength, anchorBaseRefAlleleFront));
                }
            } else if ( altSeqLength - 2 > EVENT_SIZE_THRESHOLD ){ // ref segment not long enough to merit an INV or DEL, so a fat INS, if size is enough
                final Allele fatInsertionRefAllele =
                        Allele.create(getReferenceBases(new SimpleInterval(refSegment.getContig(), refSegment.getStart(), refSegment.getEnd() - 1), reference), true);
                result.add( makeInsertion(refSegment.getContig(), refSegment.getStart(), refSegment.getEnd() - 1,
                        altSeqLength - refSegment.size(), fatInsertionRefAllele) );
            }
        }

        private static void extractFrontAndRearInsertions(final SimpleInterval refSegment, final int segmentIdx,
                                                          final List<String> altArrangement,
                                                          final Allele anchorBaseRefAlleleFront,
                                                          final Allele anchorBaseRefAlleleRear,
                                                          final List<VariantContextBuilder> result) {

            final List<Integer> segmentLen = Collections.singletonList(refSegment.size());

            final SimpleInterval frontInsPos = makeOneBpInterval(refSegment.getContig(), refSegment.getStart() - 1);
            final VariantContextBuilder frontIns =
                    getInsFromOneEnd(true, segmentIdx, frontInsPos, segmentLen, altArrangement, anchorBaseRefAlleleFront);
            if (frontIns != null)
                result.add(frontIns);

            final SimpleInterval rearInsPos = makeOneBpInterval(refSegment.getContig(), refSegment.getEnd());
            final VariantContextBuilder rearIns =
                    getInsFromOneEnd(false, segmentIdx, rearInsPos, segmentLen, altArrangement, anchorBaseRefAlleleRear);
            if (rearIns != null)
                result.add(rearIns);
        }
    }

    static final class MultiSegmentsCpxVariantExtractor extends SegmentedCpxVariantSimpleVariantExtractor {
        private static final long serialVersionUID = 1L;

        @Override
        List<VariantContext> extract(final VariantContext complexVC, final ReferenceMultiSource reference) {

            final List<SimpleInterval> refSegments =
                    getAttributeAsStringList(complexVC, GATKSVVCFConstants.CPX_SV_REF_SEGMENTS).stream()
                            .map(SimpleInterval::new)
                            .collect(Collectors.toList());

            final List<String> altArrangement = getAttributeAsStringList(complexVC, GATKSVVCFConstants.CPX_EVENT_ALT_ARRANGEMENTS);

            final Tuple3<Set<SimpleInterval>, Set<Integer>, List<Integer>> missingAndPresentAndInvertedSegments = getMissingAndPresentAndInvertedSegments(refSegments, altArrangement);
            final Set<SimpleInterval> missingSegments = missingAndPresentAndInvertedSegments._1();
            final Set<Integer> presentSegments = missingAndPresentAndInvertedSegments._2();
            final List<Integer> invertedSegments = missingAndPresentAndInvertedSegments._3();

            final List<VariantContextBuilder> result = new ArrayList<>();

            // inversions
            if ( ! invertedSegments.isEmpty() ) {
                extractInversions(reference, refSegments, presentSegments, invertedSegments, result);
            }

            // deletions
            if ( ! missingSegments.isEmpty() ){
                extractDeletions(reference, missingSegments, result);
            }

            // head and tail insertions only
            extractFrontAndRearInsertions(complexVC, refSegments, altArrangement, result);

            final String sourceID = complexVC.getID();
            final List<String> evidenceContigs = getAttributeAsStringList(complexVC, GATKSVVCFConstants.CONTIG_NAMES);

            return result.stream()
                    .map(vc -> vc.attribute(EVENT_KEY, sourceID).attribute(GATKSVVCFConstants.CONTIG_NAMES, evidenceContigs).make())
                    .collect(Collectors.toList());
        }

        private void extractInversions(final ReferenceMultiSource reference, final List<SimpleInterval> refSegmentIntervals,
                                       final Set<Integer> presentSegments, final List<Integer> invertedSegments,
                                       final List<VariantContextBuilder> result) {
            final List<VariantContextBuilder> inversions =
                    invertedSegments.stream()
                            // large enough; in addition, if both as-is and inverted versions exist, treat as insertions instead of inversions: unlike 1-segment calls, where we don't have consistency problems
                        .filter(i -> refSegmentIntervals.get(i - 1).size() > EVENT_SIZE_THRESHOLD && (!presentSegments.contains(i)))
                        .map(i -> {
                            final SimpleInterval invertedSegment = refSegmentIntervals.get(i - 1);
                            final byte[] ref = getReferenceBases(makeOneBpInterval(invertedSegment.getContig(), invertedSegment.getStart()), reference);
                            final Allele refAllele = Allele.create(ref, true);
                            return makeInversion(invertedSegment, refAllele);
                        })
                        .collect(Collectors.toList());
            result.addAll(inversions);
        }

        private void extractDeletions(final ReferenceMultiSource reference, final Set<SimpleInterval> missingSegments,
                                      final List<VariantContextBuilder> result) {
            final List<VariantContextBuilder> deletions = missingSegments.stream()
                    .filter(missingSegment -> missingSegment.size() > EVENT_SIZE_THRESHOLD) // large enough
                    .map(missingSegment -> {
                        final byte[] ref = getReferenceBases(makeOneBpInterval(missingSegment.getContig(), missingSegment.getStart()), reference);
                        final Allele refAllele = Allele.create(ref, true);
                        return makeDeletion(new SimpleInterval(missingSegment.getContig(), missingSegment.getStart(), missingSegment.getEnd() - 1), refAllele);
                    })
                    .collect(Collectors.toList());
            result.addAll(deletions);
        }

        private void extractFrontAndRearInsertions(final VariantContext complexVC, final List<SimpleInterval> refSegmentIntervals,
                                                   final List<String> altArrangement,
                                                   final List<VariantContextBuilder> result) {
            final byte[] refBases = complexVC.getReference().getBases();
            final List<Integer> refSegmentLengths = refSegmentIntervals.stream().map(SimpleInterval::size).collect(Collectors.toList());
            // index pointing to first appearance of ref segment (inverted or not) in altArrangement, from either side
            int firstRefSegmentIdx = 0; // first front
            for (final String description : altArrangement) {
                if ( descriptionIndicatesInsertion(description)) {
                    ++firstRefSegmentIdx;
                } else {
                    break;
                }
            }
            if (firstRefSegmentIdx > 0) {
                final Allele anchorBaseRefAlleleFront = Allele.create(refBases[0], true);
                final SimpleInterval startAndStop = makeOneBpInterval(complexVC.getContig(), complexVC.getStart());
                final VariantContextBuilder frontIns = getInsFromOneEnd(true, firstRefSegmentIdx, startAndStop, refSegmentLengths, altArrangement, anchorBaseRefAlleleFront);
                if (frontIns != null) result.add( frontIns );
            }

            firstRefSegmentIdx = altArrangement.size() - 1; // then end
            for (int i = altArrangement.size() - 1; i > -1 ; --i) {
                if ( descriptionIndicatesInsertion(altArrangement.get(i))) {
                    --firstRefSegmentIdx;
                } else {
                    break;
                }
            }

            if (firstRefSegmentIdx != altArrangement.size() - 1) {
                final Allele anchorBaseRefAlleleRear = Allele.create(refBases[refBases.length - 2], true);
                final SimpleInterval startAndStop = makeOneBpInterval(complexVC.getContig(), complexVC.getEnd());
                final VariantContextBuilder rearIns = getInsFromOneEnd(false, firstRefSegmentIdx, startAndStop, refSegmentLengths, altArrangement, anchorBaseRefAlleleRear);
                if (rearIns != null) result.add( rearIns );
            }
        }

        /**
         * Retrieves from the provide {@code complexVC}, reference segments described in
         * {@link GATKSVVCFConstants#CPX_SV_REF_SEGMENTS}, that are
         *   a) absent
         *   b) present as is, i.e. not inverted
         *   c) inverted
         */
        static Tuple3<Set<SimpleInterval>, Set<Integer>, List<Integer>> getMissingAndPresentAndInvertedSegments(final List<SimpleInterval> refSegments,
                                                                                                                final List<String> altArrangements ) {

            final List<Integer> invertedSegments = new ArrayList<>();
            final Set<Integer> presentSegments = new TreeSet<>();
            altArrangements
                    .forEach(s -> {
                        if ( s.startsWith("-") && ( !s.contains(":") )) { // some segment inverted
                            invertedSegments.add( Integer.valueOf(s.substring(1)) );
                        }
                        if ( !s.contains(":") && !s.startsWith(CpxVariantCanonicalRepresentation.UNMAPPED_INSERTION) && !s.startsWith("-") ) { // a ref segment, but not inverted
                            presentSegments.add(Integer.valueOf(s));
                        }
                    });

            final Set<SimpleInterval> missingSegments = IntStream.rangeClosed(1, refSegments.size()).boxed()
                    .filter(i -> !presentSegments.contains(i) && !invertedSegments.contains(i))
                    .map(i -> refSegments.get(i-1))
                    .collect(Collectors.toSet());

            return new Tuple3<>(missingSegments, presentSegments, invertedSegments);
        }

        @VisibleForTesting
        static boolean descriptionIndicatesInsertion(final String description) {
            if (description.startsWith(CpxVariantCanonicalRepresentation.UNMAPPED_INSERTION))
                return true;
            return !NumberUtils.isCreatable(description); // "(-)?[0-9]+" is describing segments, we don't count them as insertions
        }
    }

    //==================================================================================================================

    /**
     * @return {@code null} if the inserted sequence from the requested end is not over {@link #EVENT_SIZE_THRESHOLD}
     */
    @VisibleForTesting
    static VariantContextBuilder getInsFromOneEnd(final boolean fromFront, final int idxFirstMatch,
                                                  final SimpleInterval startAndStop, final List<Integer> refSegmentLengths,
                                                  final List<String> altArrangement, final Allele anchorBaseRefAllele) {
        int insLen = 0;
        if (fromFront) {
            for (int i = 0; i < idxFirstMatch; ++i) {
                insLen += getInsLen( altArrangement.get(i), refSegmentLengths );
            }
        } else {
            for (int i = idxFirstMatch + 1; i < altArrangement.size(); ++i) {
                insLen += getInsLen( altArrangement.get(i), refSegmentLengths );
            }
        }

        if (insLen > EVENT_SIZE_THRESHOLD)
            return makeInsertion(startAndStop.getContig(), startAndStop.getStart(), startAndStop.getEnd(), insLen, anchorBaseRefAllele);
        else
            return null;
    }

    @VisibleForTesting
    static int getInsLen(final String description, final List<Integer> refSegmentLengths) {
        if (description.startsWith(CpxVariantCanonicalRepresentation.UNMAPPED_INSERTION)) {
            return Integer.valueOf(description.substring(CpxVariantCanonicalRepresentation.UNMAPPED_INSERTION.length() + 1));
        } else if ( NumberUtils.isCreatable(description) ){
            final int offset = description.startsWith("-") ? 1 : 0;
            return refSegmentLengths.get( Integer.valueOf(description.substring(offset)) - 1);
        } else {
            final int offset = description.startsWith("-") ? 1 : 0;
            return new SimpleInterval(description.substring(offset)).size();
        }
    }

    // boiler-plate code block =========================================================================================

    private static Allele getAnchorBaseRefAllele(final String chr, final int pos, final ReferenceMultiSource reference) {
        return Allele.create(getReferenceBases(makeOneBpInterval(chr, pos), reference), true);
    }

    // try not to have many try's
    static byte[] getReferenceBases(final SimpleInterval interval, final ReferenceMultiSource reference) {
        try {
            return reference.getReferenceBases(interval).getBases();
        } catch (final IOException ioex) {
            throw new GATKException("Failed to extract reference bases on:" + interval, ioex);
        }
    }

    private static final Allele altSymbAlleleDel = Allele.create(SimpleSVType.createBracketedSymbAlleleString(GATKSVVCFConstants.SYMB_ALT_ALLELE_DEL));
    private static final Allele altSymbAlleleIns = Allele.create(SimpleSVType.createBracketedSymbAlleleString(GATKSVVCFConstants.SYMB_ALT_ALLELE_INS));
    private static final Allele altSymbAlleleInv = Allele.create(SimpleSVType.createBracketedSymbAlleleString(GATKSVVCFConstants.SYMB_ALT_ALLELE_INV));

    /**
     * Note that {@code delRange} is expected to be pre-process to VCF spec compatible,
     * e.g. if chr1:101-200 is deleted, then {@code delRange} should be chr1:100-200
     * @param delRange
     */
    private static VariantContextBuilder makeDeletion(final SimpleInterval delRange, final Allele refAllele) {

        return new VariantContextBuilder()
                .chr(delRange.getContig()).start(delRange.getStart()).stop(delRange.getEnd())
                .alleles(Arrays.asList(refAllele, altSymbAlleleDel))
                .id(makeID(SimpleSVType.TYPES.DEL.name(), delRange.getContig(), delRange.getStart(), delRange.getEnd()))
                .attribute(VCFConstants.END_KEY, delRange.getEnd())
                .attribute(GATKSVVCFConstants.SVLEN, - delRange.size() + 1)
                .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.DEL.name());
    }

    private static VariantContextBuilder makeInsertion(final String chr, final int pos, final int end, final int svLen,
                                                       final Allele refAllele) {

        return new VariantContextBuilder().chr(chr).start(pos).stop(end)
                .alleles(Arrays.asList(refAllele, altSymbAlleleIns))
                .id(makeID(SimpleSVType.TYPES.INS.name(), chr, pos, end))
                .attribute(VCFConstants.END_KEY, end)
                .attribute(GATKSVVCFConstants.SVLEN, svLen)
                .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.INS.name());
    }

    private static VariantContextBuilder makeInversion(final SimpleInterval invertedRegion, final Allele refAllele) {
        return new VariantContextBuilder()
                .chr(invertedRegion.getContig()).start(invertedRegion.getStart() - 1).stop(invertedRegion.getEnd())     // TODO: 5/2/18 VCF spec doesn't requst left shift by 1 for inversion POS
                .alleles(Arrays.asList(refAllele, altSymbAlleleInv))
                .id(makeID(SimpleSVType.TYPES.INV.name(), invertedRegion.getContig(), invertedRegion.getStart() - 1, invertedRegion.getEnd()))
                .attribute(VCFConstants.END_KEY, invertedRegion.getEnd())
                .attribute(GATKSVVCFConstants.SVLEN, 0)                                                                 // TODO: 5/2/18 this is following VCF spec,
                .attribute(GATKSVVCFConstants.SVTYPE, SimpleSVType.TYPES.INV.name());
    }
}
