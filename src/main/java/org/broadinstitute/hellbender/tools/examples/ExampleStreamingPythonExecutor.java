package org.broadinstitute.hellbender.tools.examples;

import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.ExampleProgramGroup;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.ReadWalker;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.utils.python.CooperativePythonScriptExecutor;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.runtime.AsynchronousStreamWriterService;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Example ReadWalker program that uses a Python streaming executor to stream summary data from a BAM
 * input file to a Python process through a FIFO. The read data is accumulated in a List until a batch
 * size threshold is reached, at which point the batch is handed off to an asynchronous write service,
 * which writes the batch to the FIFO stream on a background thread. The Python process in turn just
 * writes the data to an output file.
 *
 * <ol>
 * <li>Opens a FIFO for writing.</li>
 * <li>Creates an AsynchronousWriterService to allow writing to the FIFO in batches on a background thread</li>
 * <li>Writes a string of attributes for each read to the List until the batchSize threshold is reached.</li>
 * <li>Uses Python to read each attribute line from the FIFO, and write it to the output file.</li>
 * </ol>
 *
 * See https://github.com/broadinstitute/gatk/wiki/Writing-GATK-Tools-that-use-Python for more information
 * on using Python with GATK.
 */
@CommandLineProgramProperties(
        summary = "Example/toy program that uses a Python script.",
        oneLineSummary = "Example/toy program that uses a Python script.",
        programGroup = ExampleProgramGroup.class,
        omitFromCommandLine = true
)
public class ExampleStreamingPythonExecutor extends ReadWalker {
    private final static String NL = System.lineSeparator();

    @Argument(fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            doc = "Output file")
    private File outputFile; // output file produced by Python code

    @Argument(fullName = "batchSize",
            doc = "Size of a batch for writing")
    private int batchSize = 1000;

    // Create the Python executor. This doesn't actually start the Python process, but verifies that
    // the requested Python executable exists and can be located.
    final CooperativePythonScriptExecutor pythonExecutor = new CooperativePythonScriptExecutor(true);

    private AsynchronousStreamWriterService<String> asyncStreamWriter = null;
    private List<String> batchList = new ArrayList<>(batchSize);
    private int batchCount = 0;

    @Override
    public void onTraversalStart() {

        // Start the Python process, and get a FIFO from the executor to use to send data to Python. The lifetime
        // of the FIFO is managed by the executor; the FIFO will be destroyed when the executor is terminated.
        pythonExecutor.start(Collections.emptyList(), true);
        asyncStreamWriter = pythonExecutor.initStreamWriter(AsynchronousStreamWriterService.stringSerializer);

        // Also, ask Python to open our output file, where it will write the contents of everything it reads
        // from the FIFO. <code sendSynchronousCommand/>
        pythonExecutor.sendSynchronousCommand(String.format("tempFile = open('%s', 'w')" + NL, outputFile.getAbsolutePath()));
    }

    @Override
    public void apply(GATKRead read, ReferenceContext referenceContext, FeatureContext featureContext ) {
        // Extract data from the read and accumulate, unless we've reached a batch size, in which case we
        // kick off an asynchronous batch.
        if (batchCount == batchSize) {
            waitForPreviousBatch(); // wait for the last batch of there is one
            startNextBatch();
        }
        batchList.add(String.format(
                "Read at %s:%d-%d:\n%s\n",
                read.getContig(), read.getStart(), read.getEnd(), read.getBasesString()));
        batchCount++;
    }

    /**
     * On traversal success, write the remaining batch. Post traversal work would be done here.
     * @return Success indicator.
     */
    public Object onTraversalSuccess() {
        waitForPreviousBatch(); // wait for the previous batch, if there is one
        if (batchCount != 0) {
            // We have accumulated reads that haven't been dispatched;, so dispatch the last batch,
            // and then wait for it to complete
            startNextBatch();
            waitForPreviousBatch();
        }

        return true;
    }

    private void startNextBatch() {
        // Send an ASYNCHRONOUS command to Python to tell it to start consuming the lines about to be written
        // to the FIFO. Sending a *SYNCHRONOUS* command here would immediately block the background thread
        // since this statement will be executed BEFORE any data from the batch is written to the stream.
        pythonExecutor.sendAsynchronousCommand(
                String.format("for i in range(%s):\n    tempFile.write(tool.readDataFIFO())" + NL + NL, batchCount)
        );
        asyncStreamWriter.startAsynchronousBatchWrite(batchList);
        batchList = new ArrayList<>(batchSize);
        batchCount = 0;
    }

    private void waitForPreviousBatch() {
        if (asyncStreamWriter.waitForPreviousBatchCompletion() != null) {
            // Wait for the previous batch to be processed.
            pythonExecutor.waitForAck(); // block waiting for the ack..
        }
    }

    @Override
    public void closeTool() {
        // Terminate the async writer and Python executor in closeTool, since this always gets called.
        if (asyncStreamWriter != null) {
            asyncStreamWriter.terminate();
        }

        // Send a synchronous command to Python to close the temp file
        pythonExecutor.sendSynchronousCommand("tempFile.close()" + NL);

        // terminate the async writer and fifo
        pythonExecutor.terminateAsyncWriterForFifo();
        pythonExecutor.terminate();
    }
}
