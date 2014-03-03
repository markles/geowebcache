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
package org.geowebcache.storage.job.bdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.geowebcache.seed.GWCTask.STATE;
import org.geowebcache.storage.JobLogObject;
import org.geowebcache.storage.JobObject;
import org.geowebcache.storage.StorageBrokerTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the JobLogObject related functionality of the BDBJobStore
 * 
 * @throws Exception
 */
public class BDBJobLogStoreTest extends JobTestParent{
    private BDBJobStore store;
    private String storePath;

    private JobObject readyJob1;
    private JobObject deadJob2;
    private JobObject runningJob3;
    private JobObject readyJob4;

    private JobLogObject warningLog1;
    private JobLogObject infoLog2;
    private JobLogObject errorLog3;
    private JobLogObject warningLog4;
    private JobLogObject infoLog5;
    private JobLogObject errorLog6;

    public BDBJobLogStoreTest() throws Exception {
        storePath = StorageBrokerTest.findTempDir() + File.separator + "gwc_job_store_test";
    }

    @Before
    public void setUp() throws Exception {
        store = new BDBJobStore(storePath);
        assertEquals(0l, store.getCount());

        readyJob1 = buildJob(1, STATE.READY);
        deadJob2 = buildJob(2, STATE.DEAD);
        runningJob3 = buildJob(3, STATE.RUNNING);
        readyJob4 = buildJob(4, STATE.READY);

        warningLog1 = createDefaultWarnLog(readyJob1);
        infoLog2 = createDefaultInfoLog(deadJob2);
        errorLog3 = createDefaultErrorLog(deadJob2);
        warningLog4 = createDefaultWarnLog(readyJob4);
        infoLog5 = createDefaultInfoLog(readyJob1);
        errorLog6 = createDefaultInfoLog(deadJob2);

        store.put(readyJob1);
        store.put(deadJob2);
        store.put(runningJob3);
        store.put(readyJob4);
    }

    @After
    public void tearDown() throws Exception {
        store.clear();
        assertEquals(0l, store.getCount());
        store.close();
        File file = new File(storePath);
        file.delete();
    }

    @Test
    /**
     * This tests that we are able to retrieve all logs in the JobStore accurately using the
     * JobStore.getAllLogs() function.
     * @throws Exception
     */
    public void testAllLogs() throws Exception {
        Set<JobLogObject> expectedLogs = new HashSet<JobLogObject>();
        expectedLogs.add(warningLog1);
        expectedLogs.add(infoLog2);
        expectedLogs.add(errorLog3);
        expectedLogs.add(warningLog4);
        expectedLogs.add(infoLog5);
        expectedLogs.add(errorLog6);

        Iterable<JobLogObject> logs = store.getAllLogs();
        assertMatchingCollections(expectedLogs, logs);
    }

    @Test
    /**
     * This tests retrieving all logs for a given job, using the JobStore.getLogs(long) function.
     * @throws Exception
     */
    public void testLogsByJobId() throws Exception {
        Set<JobLogObject> job1ExpectedLogs = new HashSet<JobLogObject>();
        job1ExpectedLogs.add(warningLog1);
        job1ExpectedLogs.add(infoLog5);

        Iterable<JobLogObject> logs = store.getLogs(readyJob1.getJobId());
        assertMatchingCollections(job1ExpectedLogs, logs);

        Set<JobLogObject> job2ExpectedLogs = new HashSet<JobLogObject>();
        job2ExpectedLogs.add(infoLog2);
        job2ExpectedLogs.add(errorLog3);
        job2ExpectedLogs.add(errorLog6);

        logs = store.getLogs(deadJob2.getJobId());
        assertMatchingCollections(job2ExpectedLogs, logs);

        Set<JobLogObject> job3ExpectedLogs = new HashSet<JobLogObject>();

        logs = store.getLogs(runningJob3.getJobId());
        assertMatchingCollections(job3ExpectedLogs, logs);

        Set<JobLogObject> job4ExpectedLogs = new HashSet<JobLogObject>();
        job4ExpectedLogs.add(warningLog4);

        logs = store.getLogs(readyJob4.getJobId());
        assertMatchingCollections(job4ExpectedLogs, logs);
    }

    /**
     * This helper method traverses the actual Iterable and confirms that each JobLogObject
     * it contains is in the expected set.  It then checks the set to ensure that no additional
     * logs were expected.  This method, when successful, removes all objects from the expected set.
     * On failure, the expected set will be in an indeterminate state.
     * @param expected
     * @param actual
     */
    private void assertMatchingCollections(Set<JobLogObject> expected, Iterable<JobLogObject> actual) {
        Iterator<JobLogObject> it = actual.iterator();
        while(it.hasNext()) {
            JobLogObject job = it.next();
            assertTrue("Log " + job.getJobLogId() + " was not expected.", expected.contains(job));
            expected.remove(job);
        }
        assertTrue("Expected empty list, found " + expected.size() + " remaining items.", 0 == expected.size());

    }

}
