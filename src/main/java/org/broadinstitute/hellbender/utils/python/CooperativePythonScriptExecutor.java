package org.broadinstitute.hellbender.utils.python;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.runtime.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Python executor used to interact with a cooperative keep-alive Python process. A cooperative Python process has
 * code that conforms to the lifecycle required by this executor, and issues command acknowledgements via an "ack" FIFO
 * that is created by the executor. A second FIFO can optionally be used to pass data from GATK to the Python
 * process in batches. The lifecycle of an executor is typically:
 *
 *  - construct the executor
 *  - start the remote process ({@link #start}. This creates a fifo to be used for signalling command completion,
 *    and calls the onTraversalStart Python function, which executes the required fifo open handshake.
 *  - opitionally call {@code #initStreamWriter} to initialize and create a data transfer fifo. This executes the
 *    required fifo open handshake for the data fifo.
 *  - send one or more commands to be executed in Python
 *  - send data one or more times (write/flush to the fifo)
 *  - close the data fifo
 *  - terminate the executor {@link #terminate}, which also closes the ack FIFO
 *
 * Guidelines for writing GATK tools that use Python interactively:
 *
 *   - Program correctness should not rely on consumption of anything written by Python to stdout/stderr other than
 *     the use the prompt for synchronization via {@link #getAccumulatedOutput}. All data should be transferred through
 *     a FIFO or file.
 *   - Python code should write errors to stderr.
 *   - The FIFO should always be flushed before executing Python code that reads from it. Failure to do so can result
 *     in the Python process being blocked.
 *   - Prefer single line commands that run a script, vs. multi-line Python code embedded in Java
 *   - Always terminated with newlines (otherwise Python will block)
 *   - Terminate commands with a newline.
 *   - Try not to be chatty (maximize use of the fifo buffer by writing to it in batches before reading from Python)
 */
public class CooperativePythonScriptExecutor extends PythonExecutorBase {
    private static final Logger logger = LogManager.getLogger(StreamingPythonScriptExecutor.class);
    private static final String NL = System.lineSeparator();

    private final List<String> curatedCommandLineArgs = new ArrayList<>();

    private CooperativeProcessController spController;
    private ProcessSettings processSettings;

    private File dataTransferFIFOFile;
    private FileOutputStream dataTransferFIFOWriter;

    // Python commands that are executed in the cooperative python process
    private static String importGATK = "from gatktool import tool" + NL;
    private static String onTraversalStart = "tool.onTraversalStart('%s')" + NL;
    //private static String onTraversalSuccess = "tool.onTraversalSuccess()" + NL;
    private static String closeTool = "tool.closeTool()" + NL;
    private static String initializeDataFIFO = "tool.initializeDataFIFO('%s')" + NL;
    private static String closeDataFIFO = "tool.closeDataFIFO()" + NL;
    private static String sendAckRequest = "tool.sendAck()" + NL;

    // keep track of when an ack request has been made and reject attempts to send another ack until the
    // previous one has been handled
    private boolean isAckOutstanding = false;

    /**
     * The start method must be called to actually start the remote executable.
     *
     * @param ensureExecutableExists throw if the python executable cannot be located
     */
    public CooperativePythonScriptExecutor(boolean ensureExecutableExists) {
        this(PythonExecutableName.PYTHON, ensureExecutableExists);
    }

    /**
     * The start method must be called to actually start the remote executable.
     *
     * @param pythonExecutableName name of the python executable to start
     * @param ensureExecutableExists throw if the python executable cannot be found
     */
    public CooperativePythonScriptExecutor(final PythonExecutableName pythonExecutableName, final boolean ensureExecutableExists) {
        super(pythonExecutableName, ensureExecutableExists);
    }

    /**
     * Start the Python process.
     *
     * @param pythonProcessArgs args to be passed to the python process
     * @return true if the process is successfully started
     */
    public boolean start(final List<String> pythonProcessArgs) {
        return start(pythonProcessArgs, false);
    }

    /**
     * Start the Python process.
     *
     * @param pythonProcessArgs args to be passed to the python process
     * @param enableJournaling true to enable Journaling, which records all interprocess IO to a file. This is
     *                         expensive and should only be used for debugging purposes.
     * @return true if the process is successfully started
     */
    public boolean start(final List<String> pythonProcessArgs, final boolean enableJournaling) {
        final List<String> args = new ArrayList<>();
        args.add(externalScriptExecutableName);
        args.add("-u");
        args.add("-i");
        if (pythonProcessArgs != null) {
            args.addAll(pythonProcessArgs);
        }

        curatedCommandLineArgs.addAll(args);

        final InputStreamSettings isSettings = new InputStreamSettings();
        final OutputStreamSettings stdOutSettings = new OutputStreamSettings();
        stdOutSettings.setBufferSize(-1);
        final OutputStreamSettings stdErrSettings = new OutputStreamSettings();
        stdErrSettings.setBufferSize(-1);

        processSettings = new ProcessSettings(
                args.toArray(new String[args.size()]),
                false,  // redirect error
                null,           // directory
                null,         // env
                isSettings,
                stdOutSettings,
                stdErrSettings
        );

        // start the process, initialize the python code, do the ack fifo handshake
        spController = new CooperativeProcessController(processSettings, enableJournaling);
        final File ackFIFOFile = spController.start();
        initializeTool(ackFIFOFile);
        return true;
    }

    /**
     * Send a command to Python, and wait for a response prompt, returning all accumulated output
     * since the last call to either <link #sendSynchronousCommand/> or <line #getAccumulatedOutput/>
     * This is a blocking call, and should be used for commands that execute quickly and synchronously.
     * If no output is received from the remote process during the timeout period, an exception will be thrown.
     *
     * The caller is required to terminate commands with a newline. The executor doesn't do this
     * automatically since doing so would alter the number of prompts issued by the remote process.
     *
     * @param line data to be sent to the remote process
     * @return ProcessOutput
     * @throws UserException if a timeout occurs
     */
    public ProcessOutput sendSynchronousCommand(final String line) {
        if (!line.endsWith(NL)) {
            throw new IllegalArgumentException(
                    "Python commands must be newline-terminated in order to be executed. " +
                            "Indented Python code blocks must be terminated with additional newlines");
        }
        spController.writeProcessInput(line);
        sendAckRequest();
        return waitForAck();
    }

    /**
     * Send a command to the remote process without waiting for a response. This method should only
     * be used for responses that will block the remote process.
     *
     * NOTE: Before executing further synchronous statements after calling this method, getAccumulatedOutput
     * should be called to enforce a synchronization point.
     *
     * The caller is required to terminate commands with a newline. The executor doesn't do this
     * automatically since it can alter the number of prompts, and thus synchronization points, issued
     * by the remote process.
     *
     * @param line data to send to the remote process
     */
    public void sendAsynchronousCommand(final String line) {
        if (!line.endsWith(NL)) {
            throw new IllegalArgumentException("Python commands must be newline-terminated");
        }
        spController.writeProcessInput(line);
        sendAckRequest(); // but don't wait for it..the caller should subsequently call waitForAck
    }

    /**
     *
     * @return
     */
    public ProcessOutput waitForAck() {
        if (!isAckOutstanding) {
            throw new GATKException("An ack is already outstanding. The previous ack request must be retrieved" +
                    " before a new ack request can be issued");
        }
        final boolean isAck = spController.waitForAck();
        isAckOutstanding = false;
        // At every ack receipt, we want to retrieve the stdout/stderr output and scan for tracebacks
        final ProcessOutput po = getAccumulatedOutput();
        // if the ack was negative, throw, since the ack queue is no longer reliably in sync
        if (!isAck) {
            throw new PythonScriptExecutorException(
                    "A nack was received from the Python process (most likely caused by a raised exception): " + po.toString());
        }
        return po;
    }

    /**
     * Return a {@link AsynchronousStreamWriterService} to be used to write to an output stream, typically on a FIFO,
     * on a background thread.
     * @param streamWriter stream to which items should be written.
     * @param itemSerializer function that converts an item of type {@code T} to a {@code ByteArrayOutputStream} for serialization
     * @param <T> Type of items to be written to the stream.
     * @return {@link AsynchronousStreamWriterService}
     */
    private <T> AsynchronousStreamWriterService<T> getAsynchronousStreamWriter(
            final OutputStream streamWriter,
            final Function<T, ByteArrayOutputStream> itemSerializer)
    {
        Utils.nonNull(streamWriter);
        Utils.nonNull(itemSerializer);

        return spController.getAsynchronousStreamWriterService(streamWriter, itemSerializer);
    }

    /**
     * Return a (not necessarily executable) string representing the current command line for this executor
     * for error reporting purposes.
     * @return A string representing the command line used for this executor.
     */
    public String getApproximateCommandLine() {
        return curatedCommandLineArgs.stream().collect(Collectors.joining(" "));
    }

    /**
     *
     * @param itemSerializer
     * @param <T>
     * @return
     */
    public <T> AsynchronousStreamWriterService<T> initStreamWriter(final Function<T, ByteArrayOutputStream> itemSerializer) {
        Utils.nonNull(itemSerializer, "An item serializer must be provided for the async writer service");

        AsynchronousStreamWriterService<T> asyncWriter;
        dataTransferFIFOFile = spController.createFIFO();

        // Open the FIFO for writing. Opening a FIFO for read or write will block until there is a reader/writer
        // on the other end, so before we open it, send an ASYNCHRONOUS command, that doesn't wait for a
        // response, to the Python process to open the FIFO for reading. The Python process will then block until
        // we open the FIFO.
        sendAsynchronousCommand(String.format(initializeDataFIFO, dataTransferFIFOFile.getAbsolutePath()));
        try {
            dataTransferFIFOWriter = new FileOutputStream(dataTransferFIFOFile);
            asyncWriter = getAsynchronousStreamWriter(dataTransferFIFOWriter, itemSerializer);
        } catch ( IOException e ) {
            throw new GATKException("Failure opening FIFO for writing", e);
        }

        // synchronize on the output prompt before executing the next statement
        waitForAck();
        return asyncWriter;
    }

    /**
     *
     */
    public void terminateAsyncWriterForFifo() {
        Utils.nonNull(dataTransferFIFOFile, "The fifo has not been created or has already been terminated");
        spController.writeProcessInput(closeDataFIFO);
        sendAckRequest();
        waitForAck();
    }

    /**
     * Get the Process object associated with this executor. For testing only.
     *
     * @return
     */
    @VisibleForTesting
    protected Process getProcess() {
        return spController.getProcess();
    }
    /**
     * Terminate the remote process, closing the fifo if any.
     */
    public void terminate() {
        // we can't get an ack for this, since it closes down the ack fifo
        spController.writeProcessInput(closeTool);
        if (dataTransferFIFOWriter != null) {
            try {
                dataTransferFIFOWriter.close();
            } catch (IOException e) {
                throw new GATKException("IOException closing fifo writer", e);
            }
        }

        spController.terminate();
    }
    /**
     * Return all data accumulated since the last call to {@link #getAccumulatedOutput} (either directly, or
     * indirectly through {@link #sendSynchronousCommand}, collected until an output prompt is detected.
     *
     * Note that the output returned is somewhat non-deterministic, in that the only guaranty is that a prompt
     * was detected on either stdout or stderr. It is possible for the remote process to produce the prompt on
     * one stream (stderr or stdout), and additional output on the other; this method may detect the prompt before
     * detecting the additional output on the other stream. Such output will be retained, and returned as part of
     * the payload the next time output is retrieved.
     *
     * @return ProcessOutput containing all accumulated output from stdout/stderr
     * @throws UserException if a timeout occurs waiting for output
     * @throws PythonScriptExecutorException if a traceback is detected in the output
     */
    public ProcessOutput getAccumulatedOutput() {
        final ProcessOutput po = spController.getProcessOutput();
        final StreamOutput stdErr = po.getStderr();
        if (stdErr != null) {
            final String stdErrText = stdErr.getBufferString();
            if (stdErrText != null && stdErrText.contains("Traceback")) {
                System.out.println("Traceback on stderr");
                throw new PythonScriptExecutorException("Traceback detected: " + stdErrText);
            }
        }
//        final StreamOutput stdOut = po.getStderr();
//        if (stdOut != null) {
//            final String stdOutText = stdOut.getBufferString();
//            if (stdOutText != null && stdOutText.contains("Traceback")) {
//                System.out.println("Traceback on stdout");
//                throw new PythonScriptExecutorException("Traceback detected: " + stdOutText);
//            }
//        }

        return po;
    }

    // TODO: should this all be executed as a module main?
    private void initializeTool(final File ackFIFOFile) {
        //  first we need to import the module; no ack expected yet as we haven't initialized the ack fifo
        spController.writeProcessInput(importGATK); // no ack generated
        spController.writeProcessInput(String.format(onTraversalStart, ackFIFOFile.getAbsolutePath()));
        sendAckRequest(); // queue up an ack request
        // open the FIFO to unblock the remote caller (which should be blocked on open for read), then wait for the
        // ack to be sent
        spController.openAckFIFOForRead();
        waitForAck();
    }

    private void sendAckRequest() {
        if (isAckOutstanding) {
            throw new PythonScriptExecutorException("An ack request was made before a previously requested ack was retrieved");
        }
        spController.writeProcessInput(sendAckRequest);
        isAckOutstanding = true;
    }

}
