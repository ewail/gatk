package org.broadinstitute.hellbender.tools.spark.sv.evidence;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import htsjdk.samtools.*;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.alignment.AlignedAssembly;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVFileUtils;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVInterval;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVIntervalTree;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVUtils;
import org.broadinstitute.hellbender.utils.SequenceDictionaryUtils;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.bwa.BwaMemAlignment;
import org.broadinstitute.hellbender.utils.bwa.BwaMemAlignmentUtils;
import org.broadinstitute.hellbender.utils.fermi.FermiLiteAssembly;
import org.broadinstitute.hellbender.utils.fermi.FermiLiteAssembly.Connection;
import org.broadinstitute.hellbender.utils.fermi.FermiLiteAssembly.Contig;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * An assembly with its contigs aligned to reference, or a reason that there isn't an assembly.
 */
@DefaultSerializer(AlignedAssemblyOrExcuse.Serializer.class)
public final class AlignedAssemblyOrExcuse {
    private final int assemblyId;
    private final String errorMessage;
    private final FermiLiteAssembly assembly;
    private final List<List<BwaMemAlignment>> contigAlignments;
    private final int secondsInAssembly;
    private final float assemblyScore;
    private final int readBases;

    public int getSecondsInAssembly() { return secondsInAssembly; }
    public float getAssemblyScore() { return assemblyScore; }
    public int getReadBases() { return readBases; }

    public int getAssemblyId() {
        return assemblyId;
    }

    /**
     * Either this is null, or the assembly and list of alignments is null.
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    public boolean isNotFailure() {
        return errorMessage==null;
    }

    public FermiLiteAssembly getAssembly() {
        return assembly;
    }

    /**
     * List is equal in length to the number of contigs in the assembly.
     */
    public List<List<BwaMemAlignment>> getContigAlignments() {
        return contigAlignments;
    }

    public AlignedAssemblyOrExcuse( final int assemblyId, final String errorMessage ) {
        this.assemblyId = assemblyId;
        this.errorMessage = errorMessage;
        this.assembly = null;
        this.contigAlignments = null;
        this.secondsInAssembly = 0;
        this.assemblyScore = 0.f;
        this.readBases = 0;
    }

    public AlignedAssemblyOrExcuse( final int assemblyId, final FermiLiteAssembly assembly,
                                    final int secondsInAssembly, final float assemblyScore,
                                    final int readBases,
                                    final List<List<BwaMemAlignment>> contigAlignments ) {
        Utils.validate(assembly.getNContigs()==contigAlignments.size(),
                "Number of contigs in assembly doesn't match length of list of alignments.");
        Utils.validateArg(assembly.getContigs().stream().noneMatch(contig -> contig.getConnections()==null),
                "Some assembly has contigs that have null connections");
        this.assemblyId = assemblyId;
        this.errorMessage = null;
        this.assembly = assembly;
        this.contigAlignments = contigAlignments;
        this.secondsInAssembly = secondsInAssembly;
        this.assemblyScore = assemblyScore;
        this.readBases = readBases;
    }

    private AlignedAssemblyOrExcuse( final Kryo kryo, final Input input ) {
        this.assemblyId = input.readInt();
        this.errorMessage = input.readString();
        this.secondsInAssembly = input.readInt();
        this.assemblyScore = input.readFloat();
        this.readBases = input.readInt();
        if ( errorMessage != null ) {
            this.assembly = null;
            this.contigAlignments = null;
        } else {
            final int nContigs = input.readInt();
            final List<Contig> contigs = new ArrayList<>(nContigs);
            for ( int idx = 0; idx != nContigs; ++idx ) {
                contigs.add(readContig(input));
            }
            for ( int idx = 0; idx != nContigs; ++idx ) {
                final int nConnections = input.readInt();
                if ( nConnections == 0 ) continue;
                final List<Connection> connections = new ArrayList<>(nConnections);
                for ( int connIdx = 0; connIdx != nConnections; ++connIdx ) {
                    connections.add(readConnection(input, contigs));
                }
                contigs.get(idx).setConnections(connections);
            }
            this.assembly = new FermiLiteAssembly(contigs);
            final List<List<BwaMemAlignment>> contigAlignments = new ArrayList<>(nContigs);
            for ( int idx = 0; idx != nContigs; ++idx ) {
                final int nAlignments = input.readInt();
                final List<BwaMemAlignment> alignments = new ArrayList<>(nAlignments);
                for ( int alnIdx = 0; alnIdx != nAlignments; ++alnIdx ) {
                    alignments.add(readAlignment(input));
                }
                contigAlignments.add(alignments);
            }
            this.contigAlignments = contigAlignments;
        }
    }

    private static void writeContig( final Contig contig, final Output output ) {
        output.writeInt(contig.getSequence().length);
        output.writeBytes(contig.getSequence());
        final boolean hasCoverage = contig.getPerBaseCoverage() != null;
        output.writeBoolean(hasCoverage);
        if ( hasCoverage ) output.writeBytes(contig.getPerBaseCoverage());
        output.writeInt(contig.getNSupportingReads());
    }

    private static Contig readContig( final Input input ) {
        final int sequenceLen = input.readInt();
        final byte[] sequence = new byte[sequenceLen];
        input.readBytes(sequence);
        byte[] perBaseCoverage = null;
        if ( input.readBoolean() ) {
            perBaseCoverage = new byte[sequenceLen];
            input.readBytes(perBaseCoverage);
        }
        final int nSupportingReads = input.readInt();
        return new Contig(sequence, perBaseCoverage, nSupportingReads);
    }

    private static void writeConnection( final Connection connection,
                                         final Map<Contig, Integer> contigMap,
                                         final Output output ) {
        output.writeInt(contigMap.get(connection.getTarget()));
        output.writeInt(connection.getOverlapLen());
        output.writeBoolean(connection.isRC());
        output.writeBoolean(connection.isTargetRC());
    }

    private static Connection readConnection( final Input input, final List<Contig> contigs ) {
        final Contig target = contigs.get(input.readInt());
        final int overlapLen = input.readInt();
        final boolean isRC = input.readBoolean();
        final boolean isTargetRC = input.readBoolean();
        return new Connection(target, overlapLen, isRC, isTargetRC);
    }

    private static void writeAlignment(final BwaMemAlignment alignment, final Output output) {
        output.writeInt(alignment.getSamFlag());
        output.writeInt(alignment.getRefId());
        output.writeInt(alignment.getRefStart());
        output.writeInt(alignment.getRefEnd());
        output.writeInt(alignment.getSeqStart());
        output.writeInt(alignment.getSeqEnd());
        output.writeInt(alignment.getMapQual());
        output.writeInt(alignment.getNMismatches());
        output.writeInt(alignment.getAlignerScore());
        output.writeInt(alignment.getSuboptimalScore());
        output.writeString(alignment.getCigar());
        output.writeString(alignment.getMDTag());
        output.writeString(alignment.getXATag());
        output.writeInt(alignment.getMateRefId());
        output.writeInt(alignment.getMateRefStart());
        output.writeInt(alignment.getTemplateLen());
    }

    private static BwaMemAlignment readAlignment(final Input input) {
        final int samFlag = input.readInt();
        final int refId = input.readInt();
        final int refStart = input.readInt();
        final int refEnd = input.readInt();
        final int seqStart = input.readInt();
        final int seqEnd = input.readInt();
        final int mapQual = input.readInt();
        final int nMismatches = input.readInt();
        final int alignerScore = input.readInt();
        final int suboptimalScore = input.readInt();
        final String cigar = input.readString();
        final String mdTag = input.readString();
        final String xaTag = input.readString();
        final int mateRefId = input.readInt();
        final int mateRefStart = input.readInt();
        final int templateLen = input.readInt();
        return new BwaMemAlignment(samFlag, refId, refStart, refEnd, seqStart, seqEnd,
                mapQual, nMismatches, alignerScore, suboptimalScore,
                cigar, mdTag, xaTag,
                mateRefId, mateRefStart, templateLen);
    }

    private void serialize( final Kryo kryo, final Output output ) {
        output.writeInt(assemblyId);
        output.writeString(errorMessage);
        output.writeInt(secondsInAssembly);
        output.writeFloat(assemblyScore);
        output.writeInt(readBases);
        if ( errorMessage == null ) {
            final int nContigs = assembly.getNContigs();
            final Map<Contig, Integer> contigMap = new HashMap<>();
            output.writeInt(nContigs);
            for ( int idx = 0; idx != nContigs; ++idx ) {
                final Contig contig = assembly.getContig(idx);
                writeContig(contig, output);
                contigMap.put(contig, idx);
            }
            for ( final Contig contig : assembly.getContigs() ) {
                final List<Connection> connections = contig.getConnections();
                output.writeInt(connections.size());
                for ( final Connection connection : connections ) {
                    writeConnection(connection, contigMap, output);
                }
            }
            for ( final List<BwaMemAlignment> alignments : contigAlignments ) {
                output.writeInt(alignments.size());
                for ( final BwaMemAlignment alignment : alignments ) {
                    writeAlignment(alignment, output);
                }
            }
        }
    }

    public static final class Serializer extends com.esotericsoftware.kryo.Serializer<AlignedAssemblyOrExcuse> {
        @Override
        public void write( final Kryo kryo, final Output output, final AlignedAssemblyOrExcuse alignedAssemblyOrExcuse ) {
            alignedAssemblyOrExcuse.serialize(kryo, output);
        }

        @Override
        public AlignedAssemblyOrExcuse read( final Kryo kryo, final Input input, final Class<AlignedAssemblyOrExcuse> klass ) {
            return new AlignedAssemblyOrExcuse(kryo, input);
        }
    }

    // =================================================================================================================

    public Stream<SAMRecord> toSAMStreamForAlignmentsOfThisAssembly(final SAMFileHeader header, final List<String> refNames,
                                                                    final SAMReadGroupRecord contigAlignmentsReadGroup) {
        return IntStream.range(0, contigAlignments.size()).boxed()
                .flatMap(contigIdx ->
                        BwaMemAlignmentUtils.toSAMStreamForRead(formatContigName(assemblyId, contigIdx),
                                assembly.getContig(contigIdx).getSequence(), contigAlignments.get(contigIdx),
                                header, refNames, contigAlignmentsReadGroup)
                        );
    }

    public static String formatContigName( final int assemblyId, final int contigIdx ) {
        return formatAssemblyID(assemblyId) + ":" + formatContigID(contigIdx);
    }

    public static String formatAssemblyID( final int assemblyId ) {
        return String.format("asm%06d", assemblyId);
    }

    private static String formatContigID( final int contigIdx ) {
        return String.format("tig%05d", contigIdx);
    }

    /**
     * write a file describing each interval
     */
    public static void writeIntervalFile(final String intervalFile,
                                         final SAMFileHeader header,
                                         final List<SVInterval> intervals,
                                         final List<AlignedAssemblyOrExcuse> intervalDispositions,
                                         final SVIntervalTree<String> trueBreakpoints,
                                         final SVIntervalTree<String> narlyBreakpoints ) {
        Utils.validate(intervalFile != null && header != null &&
                        intervals != null && intervalDispositions != null,
                "At least one of the arguments is null.");

        try ( final OutputStreamWriter writer =
                      new OutputStreamWriter(new BufferedOutputStream(BucketUtils.createFile(intervalFile))) ) {
            writer.write("asm#\trefTig\tstart\tend\tnTigs\tasmSecs\tN50\tpenalty\treadLen\ttigsLen\tstatus\n");
            final List<SAMSequenceRecord> contigs = header.getSequenceDictionary().getSequences();
            final int nIntervals = intervals.size();
            for ( int intervalIdx = 0; intervalIdx != nIntervals; ++intervalIdx ) {
                // format common stuff at start of line
                final SVInterval interval = intervals.get(intervalIdx);
                Utils.nonNull(interval, "interval is null for " + intervalIdx);
                final String prefix =
                        String.format("asm%05d\t%s\t%d\t%d\t", intervalIdx,
                                contigs.get(interval.getContig()).getSequenceName(),
                                interval.getStart(), interval.getEnd());

                // format the variable material saying what happened in the assembly at the end of the line
                final AlignedAssemblyOrExcuse alignedAssemblyOrExcuse = intervalDispositions.get(intervalIdx);
                Utils.validate(alignedAssemblyOrExcuse != null &&
                                        alignedAssemblyOrExcuse.getAssemblyId() == intervalIdx,
                                "AlignedAssemblyOrExcuse list is incomplete or out of order.");
                final String disposition;
                if ( alignedAssemblyOrExcuse.getErrorMessage() != null ) {
                    disposition = "0\t0\t0\t0\t0\t0\t" + alignedAssemblyOrExcuse.getErrorMessage() + "\n";
                } else {
                    final int contigBases =
                            alignedAssemblyOrExcuse.getAssembly().getContigs().stream()
                                    .mapToInt(tig -> tig.getSequence().length).sum();

                    final boolean isCalled = narlyBreakpoints.hasOverlapper(interval);
                    final boolean isTrue = trueBreakpoints.hasOverlapper(interval);
                    final String status = (isCalled == isTrue ? "T" : "F") + (isCalled ? "P" : "N");
                    disposition =
                            String.format("%d\t%d\t%d\t%.2f\t%d\t%d\t%s\n",
                                            alignedAssemblyOrExcuse.getAssembly().getNContigs(),
                                            alignedAssemblyOrExcuse.getSecondsInAssembly(),
                                            computeN50(alignedAssemblyOrExcuse.getAssembly()),
                                            alignedAssemblyOrExcuse.getAssemblyScore(),
                                            alignedAssemblyOrExcuse.getReadBases(),
                                            contigBases,
                                            status);
                }

                writer.write(prefix + disposition);
            }
        } catch ( final IOException ioe ) {
            throw new UserException.CouldNotCreateOutputFile("Can't write intervals file " + intervalFile, ioe);
        }
    }

    //TODO: make this a method of FermiLiteAssembly
    private static int computeN50( final FermiLiteAssembly assembly ) {
        final List<Contig> contigs = assembly.getContigs();
        final int nContigs = contigs.size();
        if ( nContigs < 1 ) return 0;
        if ( nContigs == 1 ) return contigs.get(0).getSequence().length;

        final int[] lengths = new int[nContigs];
        for ( int idx = 0; idx != nContigs; ++idx ) {
            lengths[idx] = contigs.get(idx).getSequence().length;
        }
        Arrays.sort(lengths);
        final int totalSize = Arrays.stream(lengths).sum();
        int runningTotal = 0;
        for ( int idx = nContigs - 1; idx >= 0; --idx ) {
            runningTotal += 2 * lengths[idx];
            if ( runningTotal >= totalSize ) return lengths[idx];
        }
        throw new GATKException("impossible situation -- sum of array greater than twice the sum of each element");
    }

    /**
     * write a SAM file containing records for each aligned contig
     */
    static void writeAssemblySAMFile(final String outputAssembliesFile,
                                     final List<AlignedAssemblyOrExcuse> alignedAssemblyOrExcuseList,
                                     final SAMFileHeader header,
                                     final SAMFileHeader.SortOrder assemblyAlnSortOrder) {

        final String sampleId = SVUtils.getSampleId(header);
        final SAMReadGroupRecord contigAlignmentsReadGroup = new SAMReadGroupRecord(SVUtils.GATKSV_CONTIG_ALIGNMENTS_READ_GROUP_ID);
        contigAlignmentsReadGroup.setSample(sampleId);

        final SAMFileHeader cleanHeader = new SAMFileHeader(header.getSequenceDictionary());
        cleanHeader.addReadGroup(contigAlignmentsReadGroup);
        cleanHeader.setSortOrder(assemblyAlnSortOrder);

        final List<String> refNames = SequenceDictionaryUtils.getContigNamesList(cleanHeader.getSequenceDictionary());
        final Stream<SAMRecord> samRecordStream =
                alignedAssemblyOrExcuseList.stream().filter(AlignedAssemblyOrExcuse::isNotFailure)
                        .flatMap(aa -> aa.toSAMStreamForAlignmentsOfThisAssembly(cleanHeader, refNames, contigAlignmentsReadGroup));
        SVFileUtils.writeSAMFile(outputAssembliesFile, samRecordStream.iterator(), cleanHeader,
                assemblyAlnSortOrder == SAMFileHeader.SortOrder.queryname);
    }
}
