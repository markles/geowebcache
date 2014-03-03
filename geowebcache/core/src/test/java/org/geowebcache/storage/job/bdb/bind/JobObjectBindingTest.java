/**
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.geowebcache.storage.job.bdb.bind;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.geowebcache.storage.JobObject;
import org.geowebcache.storage.job.bdb.JobTestParent;
import org.junit.Test;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * Tests the encode/decode consistency of the JobObject key and entity binding classes.
 * 
 * @author mleslie
 */
public class JobObjectBindingTest extends JobTestParent {


    @Test
    public void testMarshalling() throws Exception {
        // Test the bog-standard
        JobObject job = buildDefaultJob();
        testRoundTrip(job);

        // Test a number of null field combinations
        job = buildDefaultJob();
        job.setBounds(null);
        testRoundTrip(job);

        job = buildDefaultJob();
        job.setSrs(null);
        testRoundTrip(job);

        job = buildDefaultJob();
        job.setTimeFirstStart(null);
        job.setTimeLatestStart(null);
        job.setTimeFinish(null);
        testRoundTrip(job);
    }

    private void testRoundTrip(JobObject job) throws Exception {
        TupleOutput output = new TupleOutput();
        TupleOutput keyOutput = new TupleOutput();
        JobObjectEntityBinding binding = new JobObjectEntityBinding();
        binding.objectToData(job, output);
        binding.objectToKey(job, keyOutput);
        byte[] bytes = output.toByteArray();
        byte[] keyBytes = keyOutput.toByteArray();
        assertNotNull(bytes);
        assertTrue(0 != bytes.length);
        assertNotNull(keyBytes);
        assertTrue(0 != keyBytes.length);
        TupleInput input = new TupleInput(bytes);
        TupleInput keyInput = new TupleInput(keyBytes);
        JobObject newJob = binding.entryToObject(keyInput, input);
        assertNotNull(newJob);
        assertTrue(job != newJob);
        assertEquals(job, newJob);
    }

}
