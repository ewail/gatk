package org.broadinstitute.hellbender.tools.spark.sv.discovery.inference;

import org.broadinstitute.hellbender.GATKBaseTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class SimpleVariantExtractorUnitTest extends GATKBaseTest {


    @DataProvider(name = "forTestWorkerForZeroSegmentCalls")
    private Object[][] forTestWorkerForZeroSegmentCalls() {
        final List<Object[]> data = new ArrayList<>(20);

        return data.toArray(new Object[data.size()][]);
    }

    @Test(groups = "sv", dataProvider = "forTestWorkerForZeroSegmentCalls")
    public void testWorkerForZeroSegmentCalls() {

    }
}
