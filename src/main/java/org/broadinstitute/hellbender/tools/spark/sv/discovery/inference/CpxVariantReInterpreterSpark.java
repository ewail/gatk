package org.broadinstitute.hellbender.tools.spark.sv.discovery.inference;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.http.annotation.Experimental;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.ArgumentCollection;
import org.broadinstitute.barclay.argparser.BetaFeature;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariantDiscoveryProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.engine.spark.datasources.VariantsSparkSource;
import org.broadinstitute.hellbender.tools.spark.sv.StructuralVariationDiscoveryArgumentCollection;
import org.broadinstitute.hellbender.tools.spark.sv.StructuralVariationDiscoveryPipelineSpark;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.SvDiscoveryInputData;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVIntervalTree;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVVCFWriter;

/**
 * (Internal) Tries to extract simple variants from a provided GATK-SV CPX.vcf
 */
@DocumentedFeature
@BetaFeature
@Experimental
@CommandLineProgramProperties(
        oneLineSummary = "(Internal) Tries to extract simple variants from a provided GATK-SV CPX.vcf",
        summary =
                "This tool is used in development and should not be of interest to most researchers." +
                " It is a prototype of complex structural variant re-interpretation." +
                " In particular, it tries to extract basic SVTYPE's from a user-provided GATK-SV CPX.vcf," +
                " and outputs two VCF files containing bare bone information on the simple variants.",
        programGroup = StructuralVariantDiscoveryProgramGroup.class)
public class CpxVariantReInterpreterSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;
    private final Logger localLogger = LogManager.getLogger(CpxVariantReInterpreterSpark.class);

    @Override
    public boolean requiresReference() {
        return true;
    }

    @Override
    public boolean requiresReads() {
        return true;
    }

    @ArgumentCollection
    private StructuralVariationDiscoveryArgumentCollection.DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection
            discoverStageArgs
            = new StructuralVariationDiscoveryArgumentCollection.DiscoverVariantsFromContigsAlignmentsSparkArgumentCollection();

    @Argument(doc = "file containing non-canonical chromosome names (e.g chrUn_KI270588v1) in the reference, human reference (hg19 or hg38) assumed when omitted",
            shortName = "alt-tigs",
            fullName = "non-canonical-contig-names-file", optional = true)
    public String nonCanonicalChromosomeNamesFile;

    @Argument(doc = "file containing complex variants as output by GATK-SV",
            fullName = "cpx-vcf")
    private String complexVCF;

    @Argument(doc = "prefix for two files containing derived simple variants for complex variants having one/multiple entry in SEGMENT annotation",
            fullName = "prefix-out-vcf")
    private String derivedSimpleVCFPrefix;

    @Override
    protected void runTool(final JavaSparkContext ctx) {

        final SAMFileHeader headerForReads = getHeaderForReads();
        final SAMSequenceDictionary refSeqDictionary = headerForReads.getSequenceDictionary();
        final Broadcast<SVIntervalTree<VariantContext>> cnvCallsBroadcast =
                StructuralVariationDiscoveryPipelineSpark.broadcastCNVCalls(ctx, headerForReads, discoverStageArgs.cnvCallsFile);
        final SvDiscoveryInputData svDiscoveryInputData =
                new SvDiscoveryInputData(ctx, discoverStageArgs, derivedSimpleVCFPrefix,
                        null, null, null,
                        cnvCallsBroadcast,
                        getUnfilteredReads(), headerForReads, getReference(), localLogger);

        final JavaRDD<VariantContext> complexVariants = new VariantsSparkSource(ctx)
                .getParallelVariantContexts(complexVCF, getIntervals());

        final SimpleVariantExtractor.ExtractedSimpleVariants extract =
                SimpleVariantExtractor.extract(complexVariants, svDiscoveryInputData, nonCanonicalChromosomeNamesFile);

        final String derivedOneSegmentSimpleVCF = derivedSimpleVCFPrefix + "_1_seg.vcf";
        final String derivedMultiSegmentSimpleVCF = derivedSimpleVCFPrefix + "_multi_seg.vcf";
        SVVCFWriter.writeVCF(extract.reInterpretZeroOrOneSegmentCalls, derivedOneSegmentSimpleVCF, refSeqDictionary, logger);
        SVVCFWriter.writeVCF(extract.reInterpretMultiSegmentsCalls, derivedMultiSegmentSimpleVCF, refSeqDictionary, logger);
    }
}
