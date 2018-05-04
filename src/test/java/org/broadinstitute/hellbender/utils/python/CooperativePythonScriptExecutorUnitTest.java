package org.broadinstitute.hellbender.utils.python;

import htsjdk.samtools.util.BufferedLineReader;
import org.broadinstitute.hellbender.GATKBaseTest;
import org.broadinstitute.hellbender.utils.runtime.AsynchronousStreamWriterService;
import org.broadinstitute.hellbender.utils.runtime.ProcessOutput;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// NOTE: Python has bugs where it sometimes prints the prompt to stdout, and sometimes to stderr:
//
// https://bugs.python.org/issue17620
// https://bugs.python.org/issue1927
//
// Beware TestNG has a bug where it throws ArrayIndexOutOfBoundsException instead of TimeoutException
// exception when the test time exceeds the timeOut threshold. This is fixed but not yet released:
//
//  https://github.com/cbeust/testng/issues/1493
//
public class CooperativePythonScriptExecutorUnitTest extends GATKBaseTest {
    private static final String NL = System.lineSeparator();

    @DataProvider(name="supportedPythonVersions")
    public Object[][] getSupprtedPythonExecutableNames() {
        return new Object[][] {
                { PythonScriptExecutor.PythonExecutableName.PYTHON },
                { PythonScriptExecutor.PythonExecutableName.PYTHON3 },
        };
    }

    @Test(groups = "python", dataProvider="supportedPythonVersions")
    public void testPythonExists(final PythonScriptExecutor.PythonExecutableName executableName) {
        Assert.assertTrue(
                new CooperativePythonScriptExecutor(executableName,true).externalExecutableExists(),
                "Python not found in environment ${PATH}"
        );
    }

    @Test(groups = "python", dataProvider="supportedPythonVersions", dependsOnMethods = "testPythonExists", timeOut=10000)
    public void testExecuteCommand(final PythonScriptExecutor.PythonExecutableName executableName) throws IOException {
        // create a temporary output file
        final File tempFile = createTempFile("pythonExecuteCommandTest", "txt");
        final String WRITE_FILE_SCRIPT =
                String.format(
                        "with open('%s', 'w') as tempFile:\n    tempFile.write('Hello world')" + NL + NL,
                        tempFile.getAbsolutePath());
        final String CLOSE_FILE_SCRIPT = "tempFile.close()" + NL;

        final CooperativePythonScriptExecutor cooperativePythonExecutor =
                new CooperativePythonScriptExecutor(executableName,true);
        Assert.assertNotNull(cooperativePythonExecutor);

        Assert.assertTrue(cooperativePythonExecutor.start(Collections.emptyList()));

        try {
            cooperativePythonExecutor.sendSynchronousCommand(WRITE_FILE_SCRIPT);
            cooperativePythonExecutor.sendSynchronousCommand(CLOSE_FILE_SCRIPT);
        }
        finally {
            cooperativePythonExecutor.terminate();
            Assert.assertFalse(cooperativePythonExecutor.getProcess().isAlive());
        }

        // read the temp file in and validate
        try (final FileInputStream fis= new FileInputStream(tempFile);
             final BufferedLineReader br = new BufferedLineReader(fis)) {
            Assert.assertEquals(br.readLine(), "Hello world");
        }
    }

    @Test(groups = "python", dataProvider="supportedPythonVersions", dependsOnMethods = "testPythonExists")
    public void testStderrOutput(final PythonScriptExecutor.PythonExecutableName executableName) {
        // test write to stderr from python
        final CooperativePythonScriptExecutor cooperativePythonExecutor =
                new CooperativePythonScriptExecutor(executableName,true);
        Assert.assertNotNull(cooperativePythonExecutor);
        Assert.assertTrue(cooperativePythonExecutor.start(Collections.emptyList()));

        try {
            cooperativePythonExecutor.sendSynchronousCommand("import sys" + NL);
            final ProcessOutput po = cooperativePythonExecutor.sendSynchronousCommand(
                    "sys.stderr.write('error output to stderr\\n')" + NL);

            Assert.assertNotNull(po.getStderr());
            Assert.assertNotNull(po.getStderr().getBufferString());
            Assert.assertNotNull(po.getStderr().getBufferString().contains("error output to stderr"));
        }
        finally {
            cooperativePythonExecutor.terminate();
            Assert.assertFalse(cooperativePythonExecutor.getProcess().isAlive());
        }
    }

    @Test(groups = "python", dataProvider="supportedPythonVersions", dependsOnMethods = "testPythonExists")
    public void testTerminateWhilePythonBlocked(final PythonScriptExecutor.PythonExecutableName executableName) {
        // Test termination on a Python process that is blocked on I/O to ensure that we don't leave
        // zombie Python processes around. This unfinished code block will cause Python to hang with
        // a continuation prompt writing for more input, and must be sent to Python asynchronously.
        final String UNTERMINATED_SCRIPT = "with open('nonexistent.txt', 'w') as foo:" + NL;
        final CooperativePythonScriptExecutor cooperativePythonExecutor =
                new CooperativePythonScriptExecutor(executableName, true);
        Assert.assertTrue(cooperativePythonExecutor.start(Collections.emptyList()));

        try {
            // send the output asynchronously so *we* don't block
            cooperativePythonExecutor.sendAsynchronousCommand(UNTERMINATED_SCRIPT);
        }
        finally {
            cooperativePythonExecutor.terminate();
            Assert.assertFalse(cooperativePythonExecutor.getProcess().isAlive());
        }
    }

    @Test(groups = "python", dataProvider="supportedPythonVersions", dependsOnMethods = "testPythonExists", expectedExceptions = RuntimeException.class)
    public void testTryTerminatedController(final PythonScriptExecutor.PythonExecutableName executableName) {
        final CooperativePythonScriptExecutor cooperativePythonExecutor =
                new CooperativePythonScriptExecutor(executableName, true);
        Assert.assertTrue(cooperativePythonExecutor.start(Collections.emptyList()));

        cooperativePythonExecutor.terminate();
        Assert.assertFalse(cooperativePythonExecutor.getProcess().isAlive());

        cooperativePythonExecutor.sendAsynchronousCommand("bogus command = controller is already terminated\n");
    }

    @Test(groups = "python", dataProvider="supportedPythonVersions", dependsOnMethods = "testPythonExists", expectedExceptions = IllegalArgumentException.class)
    public void testRequireNewlineTerminatedCommand(final PythonScriptExecutor.PythonExecutableName executableName) {
        final String NO_NEWLINE_SCRIPT = "print 'hello, world'";
        final CooperativePythonScriptExecutor cooperativePythonExecutor =
                new CooperativePythonScriptExecutor(executableName, true);
        Assert.assertTrue(cooperativePythonExecutor.start(Collections.emptyList()));
        try {
            cooperativePythonExecutor.sendAsynchronousCommand(NO_NEWLINE_SCRIPT);
        }
        finally {
            cooperativePythonExecutor.terminate();
            Assert.assertFalse(cooperativePythonExecutor.getProcess().isAlive());
        }
    }

    @Test(groups = "python", dataProvider="supportedPythonVersions", dependsOnMethods = "testPythonExists", timeOut=10000, invocationCount = 10)
    public void testAsyncWriteService(final PythonScriptExecutor.PythonExecutableName executableName) throws IOException {
        // Python script statements to open, read, and write to and from the FIFO and temporary files
        final String PYTHON_OPEN_TEMP_FILE      = "tempFile = open('%s', 'w')" + NL;
        final String PYTHON_TRANSFER_FIFO_TO_TEMP_FILE = "for i in range(%s):\n    tempFile.write(tool.readDataFIFO())" + NL + NL;
        final String PYTHON_CLOSE_TEMP_FILE     = "tempFile.close()" + NL;
        final String ROUND_TRIP_DATA = "round trip data (%s)" + NL;

        final List<String> linesWrittenToFIFO = new ArrayList<>();

        final CooperativePythonScriptExecutor cooperativePythonExecutor =
                new CooperativePythonScriptExecutor(executableName, false);
        Assert.assertNotNull(cooperativePythonExecutor);
        Assert.assertTrue(cooperativePythonExecutor.start(Collections.emptyList()));

        // create a temporary file
        final File tempFile = createTempFile("pythonRoundTripTest", "txt");

        // get an async writer; write data to the fifo and round trip to python ROUND_TRIP_COUNT times
        final AsynchronousStreamWriterService<String>asyncWriter =
                cooperativePythonExecutor.initStreamWriter(AsynchronousStreamWriterService.stringSerializer);

        try {
            // ask python to open the temp file
            cooperativePythonExecutor.sendSynchronousCommand(String.format(PYTHON_OPEN_TEMP_FILE, tempFile.getAbsolutePath()));

            final int ROUND_TRIP_COUNT = 100*1024;
            final int syncFrequency = 1024;

            List<String> fifoData = new ArrayList<>(syncFrequency);
            int count = 0;

            for (int i = 0; i < ROUND_TRIP_COUNT; i++) {
                if (i != 0 && (i % syncFrequency) == 0) {
                    // wait for the last batch to complete before we start a new one
                    if (asyncWriter.waitForPreviousBatchCompletion() != null) {
                        cooperativePythonExecutor.waitForAck();
                    }
                    cooperativePythonExecutor.sendAsynchronousCommand(String.format(PYTHON_TRANSFER_FIFO_TO_TEMP_FILE, count));
                    asyncWriter.startAsynchronousBatchWrite(fifoData);
                    count = 0;
                    fifoData = new ArrayList<>(syncFrequency);
                }

                // save the line to be written to the fifo
                final String roundTripLine = String.format(ROUND_TRIP_DATA, i);
                fifoData.add(roundTripLine);
                // keep track of everything we write to the FIFO so we can validate at the end of the test
                linesWrittenToFIFO.add(roundTripLine);
                count++;
            }

            // wait for the writing to complete
            asyncWriter.waitForPreviousBatchCompletion();
            // and wait for the reading to complete..
            cooperativePythonExecutor.waitForAck();

            if (fifoData.size() != 0) {
                cooperativePythonExecutor.sendAsynchronousCommand(String.format(PYTHON_TRANSFER_FIFO_TO_TEMP_FILE, count));
                asyncWriter.startAsynchronousBatchWrite(fifoData);
                // wait for the writing to complete
                asyncWriter.waitForPreviousBatchCompletion();
                // and wait for the reading to complete..
                cooperativePythonExecutor.waitForAck();
            }
            // tell Python to close the temp file
            cooperativePythonExecutor.sendSynchronousCommand(PYTHON_CLOSE_TEMP_FILE);
            //Assert.assertTrue(asyncWriter.terminate());
            Assert.assertEquals(linesWrittenToFIFO.size(), ROUND_TRIP_COUNT);
        }
        finally {
            if (asyncWriter != null) {
                Assert.assertTrue(asyncWriter.terminate());
            }
            cooperativePythonExecutor.terminate();
            Assert.assertFalse(cooperativePythonExecutor.getProcess().isAlive());
        }

        // read the temp file in and make sure everything written to the FIFO was round-tripped by Python,
        // including the round-trip count
        try (final FileInputStream fis= new FileInputStream(tempFile);
             final BufferedLineReader br = new BufferedLineReader(fis)) {
            linesWrittenToFIFO.forEach(expectedLine -> Assert.assertEquals(br.readLine() + NL, expectedLine));
        }
    }

    @Test(groups = "python", dataProvider="supportedPythonVersions", dependsOnMethods = "testPythonExists", expectedExceptions = PythonScriptExecutorException.class)
    public void testPythonAssert(final PythonScriptExecutor.PythonExecutableName executableName) {
        final CooperativePythonScriptExecutor cooperativePythonExecutor =
                new CooperativePythonScriptExecutor(executableName,true);
        Assert.assertNotNull(cooperativePythonExecutor);
        Assert.assertTrue(cooperativePythonExecutor.start(Collections.emptyList(), true));

        try {
            cooperativePythonExecutor.sendSynchronousCommand("assert false" + NL);
        }
        finally {
            cooperativePythonExecutor.terminate();
            Assert.assertFalse(cooperativePythonExecutor.getProcess().isAlive());
        }
    }

}
