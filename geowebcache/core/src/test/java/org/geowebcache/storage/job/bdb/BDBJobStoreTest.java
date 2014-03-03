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

import static org.junit.Assert.*;

import java.io.File;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.seed.GWCTask.STATE;
import org.geowebcache.storage.JobObject;
import org.geowebcache.storage.StorageBrokerTest;
import org.geowebcache.storage.StorageException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the JobObject related functionality of the BDBJobStore
 * 
 * @throws Exception
 */
public class BDBJobStoreTest extends JobTestParent {
    private static final Log log = LogFactory.getLog(BDBJobStoreTest.class);
    private String storePath;

    private BDBJobStore store;

    private JobObject readyJob1;
    private JobObject unsetJob2;
    private JobObject readyJob3;
    private JobObject deadJob4;
    private JobObject readyJob5;
    private JobObject interruptedJob6;
    private JobObject doneJob7;
    private JobObject runningJob8;
    private JobObject killedJob9;
    private JobObject readyJobA;
    private JobObject unsetJobTimeless;

    private long baseTime;
    private long tenMinutes;

    public BDBJobStoreTest() throws Exception {
        storePath = StorageBrokerTest.findTempDir() + File.separator + "gwc_job_store_test";
    }


    @Before
    public void setUp() throws Exception {
        tenMinutes = 1000 * 60 * 10;
        baseTime = new Date().getTime() - (tenMinutes * 20);

        store = new BDBJobStore(storePath);
        assertEquals(0l, store.getCount());

        readyJob1 = buildAndPersist(1, STATE.READY, new Timestamp(baseTime + (tenMinutes * 1)));
        unsetJob2 = buildAndPersist(2, STATE.UNSET, new Timestamp(baseTime + (tenMinutes * 2)));
        readyJob3 = buildAndPersist(3, STATE.READY, new Timestamp(baseTime + (tenMinutes * 3)));
        deadJob4 = buildAndPersist(4, STATE.DEAD, new Timestamp(baseTime + (tenMinutes * 4)));
        readyJob5 = buildAndPersist(5, STATE.READY, new Timestamp(baseTime + (tenMinutes * 5)));
        interruptedJob6 = buildAndPersist(6, STATE.INTERRUPTED, new Timestamp(baseTime + (tenMinutes * 6)));
        doneJob7 = buildAndPersist(7, STATE.DONE, new Timestamp(baseTime + (tenMinutes * 7)));
        runningJob8 = buildAndPersist(8, STATE.RUNNING, new Timestamp(baseTime + (tenMinutes * 8)));
        killedJob9 = buildAndPersist(9, STATE.KILLED, new Timestamp(baseTime + (tenMinutes * 9)));
        readyJobA = buildAndPersist(10, STATE.READY, new Timestamp(baseTime + (tenMinutes * 10)));
        unsetJobTimeless = buildAndPersist(11, STATE.UNSET, null);
    }

    /**
     * Creates a job object using the given parameters and puts it into the JobStore.
     * 
     * @param id
     * @param state
     * @param timeFinish
     * @return
     * @throws GeoWebCacheException
     * @throws StorageException
     */
    private JobObject buildAndPersist(long id, STATE state, Timestamp timeFinish) 
            throws GeoWebCacheException, StorageException {
        JobObject job = buildJob(id, state, timeFinish);
        store.put(job);
        assertTrue(job.getJobId() != -1);
        return job;
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
    public void testCount() throws Exception {
        assertEquals(11, store.getCount());
    }

    @Test
    public void testGet() throws Exception {
        Set<JobObject> expected = new HashSet<JobObject>();
        expected.add(readyJob1);
        expected.add(unsetJob2);
        expected.add(deadJob4);
        expected.add(readyJob5);
        expected.add(interruptedJob6);
        expected.add(doneJob7);
        expected.add(runningJob8);
        expected.add(killedJob9);
        expected.add(readyJobA);
        expected.add(unsetJobTimeless);

        Iterator<JobObject> it = expected.iterator();
        while(it.hasNext()) {
            JobObject job = it.next();
            JobObject newJob = new JobObject();
            newJob.setJobId(job.getJobId());

            assertTrue("Check that the jobs are not yet equal.", !job.equals(newJob));
            assertTrue("We should find this job.", store.get(newJob));
            assertTrue("Make sure I haven't done something stupid.", job != newJob);
            assertEquals("Jobs should now equal.", job, newJob);
        }

        JobObject newJob = new JobObject();
        newJob.setJobId(Long.MIN_VALUE);
        assertFalse("This job doesn't exist.", store.get(newJob));
    }

    @Test
    public void testDeletion() throws Exception {
        Set<JobObject> expected = new HashSet<JobObject>();
        expected.add(readyJob1);
        expected.add(unsetJob2);
        expected.add(deadJob4);
        expected.add(readyJob5);
        expected.add(interruptedJob6);
        expected.add(doneJob7);
        expected.add(runningJob8);
        expected.add(killedJob9);
        expected.add(readyJobA);
        expected.add(unsetJobTimeless);
        assertTrue(store.delete(readyJob3.getJobId()));
        Iterable<JobObject> result = store.getJobs();
        assertMatchingCollections(expected, result);

        expected = new HashSet<JobObject>();
        expected.add(readyJob1);
        expected.add(unsetJob2);
        expected.add(deadJob4);
        expected.add(readyJob5);
        expected.add(interruptedJob6);
        expected.add(runningJob8);
        expected.add(killedJob9);
        expected.add(readyJobA);
        expected.add(unsetJobTimeless);
        assertTrue(store.delete(doneJob7.getJobId()));
        result = store.getJobs();
        assertMatchingCollections(expected, result);

        expected = new HashSet<JobObject>();
        expected.add(readyJob1);
        expected.add(unsetJob2);
        expected.add(deadJob4);
        expected.add(readyJob5);
        expected.add(interruptedJob6);
        expected.add(runningJob8);
        expected.add(killedJob9);
        expected.add(readyJobA);
        expected.add(unsetJobTimeless);
        assertFalse(store.delete(readyJob3.getJobId()));
        result = store.getJobs();
        assertMatchingCollections(expected, result);
    }

    @Test
    public void testPurgeOldJobs() throws Exception {
        Timestamp time = new Timestamp(baseTime - 1);
        Set<JobObject> expected = new HashSet<JobObject>();
        expected.add(readyJob1);
        expected.add(unsetJob2);
        expected.add(readyJob3);
        expected.add(deadJob4);
        expected.add(readyJob5);
        expected.add(interruptedJob6);
        expected.add(doneJob7);
        expected.add(runningJob8);
        expected.add(killedJob9);
        expected.add(readyJobA);
        expected.add(unsetJobTimeless);
        assertEquals(0l, store.purgeOldJobs(time));
        Iterable<JobObject> remaining = store.getJobs();
        assertMatchingCollections(expected, remaining);

        time = new Timestamp(baseTime + ((long)(tenMinutes * 5.5)));
        expected.clear();
        expected.add(interruptedJob6);
        expected.add(doneJob7);
        expected.add(runningJob8);
        expected.add(killedJob9);
        expected.add(readyJobA);
        expected.add(unsetJobTimeless);

        assertEquals(5l, store.purgeOldJobs(time));

        remaining = store.getJobs();
        assertMatchingCollections(expected, remaining);

        time = new Timestamp(new Date().getTime());
        expected.clear();
        expected.add(unsetJobTimeless);

        assertEquals(5l, store.purgeOldJobs(time));

        remaining = store.getJobs();
        assertMatchingCollections(expected, remaining);
    }

    @Test
    public void testJobRetrieval() throws Exception {
        JobObject job = new JobObject();
        job.setJobId(readyJob1.getJobId());
        assertTrue(store.get(job));
        assertEquals(readyJob1, job);

        job = new JobObject();
        job.setJobId(unsetJob2.getJobId());
        assertTrue(store.get(job));
        assertEquals(unsetJob2, job);


        job = new JobObject();
        job.setJobId(readyJob3.getJobId());
        assertTrue(store.get(job));
        assertEquals(readyJob3, job);

        job = new JobObject();
        job.setJobId(deadJob4.getJobId());
        assertTrue(store.get(job));
        assertEquals(deadJob4, job);

        job = new JobObject();
        job.setJobId(readyJob5.getJobId());
        assertTrue(store.get(job));
        assertEquals(readyJob5, job);


        job = new JobObject();
        job.setJobId(interruptedJob6.getJobId());
        assertTrue(store.get(job));
        assertEquals(interruptedJob6, job);

        job = new JobObject();
        job.setJobId(doneJob7.getJobId());
        assertTrue(store.get(job));
        assertEquals(doneJob7, job);

        job = new JobObject();
        job.setJobId(runningJob8.getJobId());
        assertTrue(store.get(job));
        assertEquals(runningJob8, job);

        job = new JobObject();
        job.setJobId(killedJob9.getJobId());
        assertTrue(store.get(job));
        assertEquals(killedJob9, job);

        job = new JobObject();
        job.setJobId(readyJobA.getJobId());
        assertTrue(store.get(job));
        assertEquals(readyJobA, job);

        job = new JobObject();
        job.setJobId(unsetJobTimeless.getJobId());
        assertTrue(store.get(job));
        assertEquals(unsetJobTimeless, job);
    }


    @Test
    /**
     * In this test, we want to actually close and reopen the database and check that 
     * everything is there.
     * @throws Exception
     */

    public void testDiskPersistence() throws Exception {
        store.close();
        store = null;
        store = new BDBJobStore(storePath);

        testJobRetrieval();
    }

    private void assertMatchingCollections(Set<JobObject> expected, Iterable<JobObject> actual) {
        Iterator<JobObject> it = actual.iterator();
        while(it.hasNext()) {
            JobObject job = it.next();
            assertTrue("Job " + job.getJobId() + " was not expected.", expected.contains(job));
            expected.remove(job);
        }
        assertTrue("Expected empty list, found " + expected.size() + " remaining items.", 0 == expected.size());

    }

    @Test
    public void testAllJobs() throws Exception {
        Set<JobObject> expectedJobs = new HashSet<JobObject>();
        expectedJobs.add(readyJob1);
        expectedJobs.add(unsetJob2);
        expectedJobs.add(readyJob3);
        expectedJobs.add(deadJob4);
        expectedJobs.add(readyJob5);
        expectedJobs.add(interruptedJob6);
        expectedJobs.add(doneJob7);
        expectedJobs.add(runningJob8);
        expectedJobs.add(killedJob9);
        expectedJobs.add(readyJobA);
        expectedJobs.add(unsetJobTimeless);

        Iterable<JobObject> jobs = store.getJobs();
        assertMatchingCollections(expectedJobs, jobs);
    }

    @Test
    public void testPendingJobs() throws Exception {
        Set<JobObject> expectedJobs = new HashSet<JobObject>();
        expectedJobs.add(readyJob1);
        expectedJobs.add(unsetJob2);
        expectedJobs.add(readyJob3);
        expectedJobs.add(readyJob5);
        expectedJobs.add(readyJobA);
        expectedJobs.add(unsetJobTimeless);

        Iterable<JobObject> jobs = store.getPendingScheduledJobs();
        assertMatchingCollections(expectedJobs, jobs);
    }

    @Test
    public void testInterruptedJobs() throws Exception {
        Set<JobObject> expectedJobs = new HashSet<JobObject>();
        expectedJobs.add(deadJob4);
        expectedJobs.add(interruptedJob6);

        Iterable<JobObject> jobs = store.getInterruptedJobs();
        assertMatchingCollections(expectedJobs, jobs);
    }

}
