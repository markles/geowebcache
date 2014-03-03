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

import org.geowebcache.storage.JobLogObject;
import org.geowebcache.storage.job.bdb.JobTestParent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * Tests the functionality and encode/decode consistency of JobLogObject entity and key
 * binding classes.
 * @author mleslie
 *
 */
public class JobLogObjectBindingTest extends JobTestParent {

    private JobLogObject warningLog1;
    private JobLogObject infoLog2;
    private JobLogObject errorLog3;
    private JobLogObject warningLog4;
    private JobLogObject infoLog5;
    private JobLogObject errorLog6;


    @Before
    public void setUp() throws Exception {
        long id = -1;
        warningLog1 = createDefaultWarnLog(id++);
        warningLog1.setJobLogId(id++);
        infoLog2 = createDefaultInfoLog(id++);
        infoLog2.setJobLogId(id++);
        errorLog3 = createDefaultErrorLog(id++);
        errorLog3.setJobLogId(id++);
        warningLog4 = createDefaultWarnLog(Long.MAX_VALUE);
        warningLog4.setJobLogId(id++);
        infoLog5 = createDefaultInfoLog(Long.MIN_VALUE);
        infoLog5.setJobLogId(id++);
        errorLog6 = createDefaultInfoLog(10l);
        errorLog6.setJobLogId(id++);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testMarshalling() throws Exception {
        testRoundTrip(warningLog1);
        testRoundTrip(infoLog2);
        testRoundTrip(errorLog3);
        testRoundTrip(warningLog4);
        testRoundTrip(infoLog5);
        testRoundTrip(errorLog6);
    }

    private void testRoundTrip(JobLogObject log) throws Exception {
        TupleOutput output = new TupleOutput();
        TupleOutput keyOutput = new TupleOutput();

        JobLogObjectEntityBinding binding = new JobLogObjectEntityBinding();
        binding.objectToData(log, output);
        binding.objectToKey(log, keyOutput);
        byte[] bytes = output.toByteArray();
        byte[] keyBytes = keyOutput.toByteArray();

        assertNotNull("Data encoding must not be null.", bytes);
        assertTrue("Must return some data bytes.", 0 != bytes.length);
        assertNotNull("Key encoding must not be null.", keyBytes);
        assertTrue("Must return some key bytes.", 0 != keyBytes.length);

        TupleInput input = new TupleInput(bytes);
        TupleInput keyInput = new TupleInput(keyBytes);
        JobLogObject newLog = binding.entryToObject(keyInput, input);
        assertNotNull("Decoded log must not be null.", newLog);
        assertTrue("Decoded log must be distinct from original.", log != newLog);
        assertEquals("Decoded log must be equivalent to original.", log, newLog);
    }

}
