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
package org.geowebcache.rest.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.net.URL;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.geowebcache.config.XMLConfiguration;
import org.geowebcache.config.XMLConfigurationBackwardsCompatibilityTest;
import org.geowebcache.grid.GridSetBroker;
import org.geowebcache.job.JobDispatcher;
import org.geowebcache.layer.TileLayerDispatcher;
import org.geowebcache.seed.SeederThreadPoolExecutor;
import org.geowebcache.seed.TileBreeder;
import org.geowebcache.storage.JobLogObject;
import org.geowebcache.storage.JobLogObject.LOG_LEVEL;
import org.geowebcache.storage.JobObject;
import org.geowebcache.storage.job.bdb.BDBJobStore;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.restlet.data.MediaType;
import org.restlet.resource.InputRepresentation;
import org.restlet.resource.Representation;
import org.restlet.resource.StringRepresentation;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.HierarchicalStreamDriver;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.copy.HierarchicalStreamCopier;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;
import com.thoughtworks.xstream.io.xml.DomDriver;
import com.thoughtworks.xstream.io.xml.PrettyPrintWriter;

/**
 * Provides helper methods and setup common to the job and job log restlet test classes.
 * 
 * @author mleslie
 */
public abstract class AbstractJobRestletTest {

    private String storePath;
    private BDBJobStore store;
    private File configDir;
    private File configFile;
    private GridSetBroker gridSetBroker;
    private XMLConfiguration config;
    protected JobRestlet jobRestlet;
    protected JobLogRestlet jobLogRestlet;

    protected JobObject job1Ready;
    protected JobObject job2Running;
    protected JobObject job3Done;
    
    protected JobLogObject log01;
    protected JobLogObject log02;
    protected JobLogObject log03;
    protected JobLogObject log04;
    protected JobLogObject log05;
    protected JobLogObject log06;
    protected JobLogObject log07;
    protected JobLogObject log08;
    protected JobLogObject log09;
    protected JobLogObject log10;
    

    @Before
    public void setUp() throws Exception {
        String configBaseDir = JobRestletJobsTest.findTempDir();

        configDir = new File(configBaseDir, "testConfig");
        FileUtils.deleteDirectory(configDir);
        configDir.mkdirs();
        URL source = XMLConfiguration.class
                .getResource(XMLConfigurationBackwardsCompatibilityTest.LATEST_FILENAME);
        configFile = new File(configDir, "geowebcache.xml");
        FileUtils.copyURLToFile(source, configFile);

        gridSetBroker = new GridSetBroker(true, true);
        config = new XMLConfiguration(null, configDir.getAbsolutePath());
        config.initialize(gridSetBroker);

        storePath = configBaseDir + File.separator + "gwc_job_store_test";
        store = new BDBJobStore(storePath);

        SeederThreadPoolExecutor executor = new SeederThreadPoolExecutor(1, 3) {
            @Override
            public <T> Future<T> submit(Callable<T> task) {
                //We don't actually want to start anything.
                return getFuture();
            }
            @Override
            public Future<?> submit(Runnable task) {
                //We don't actually want to start anything.
                return getFuture();
            }
            public <T extends Object> java.util.concurrent.Future<T> submit(Runnable task, T result) {
                //We don't actually want to start anything.
                return getFuture();
            };
            private <T> Future<T> getFuture() {
                return new Future<T>() {
                    public boolean cancel(boolean mayInterruptIfRunning) {
                        return true;
                    }

                    public T get() throws InterruptedException,
                    ExecutionException {
                        return null;
                    }

                    public T get(long timeout, TimeUnit unit)
                            throws InterruptedException, ExecutionException,
                            TimeoutException {
                        return null;
                    }

                    public boolean isCancelled() {
                        return false;
                    }

                    public boolean isDone() {
                        return true;
                    }
                };
            }
        };
        TileLayerDispatcher dispatcher = new TileLayerDispatcher(gridSetBroker);
        dispatcher.addConfiguration(config);

        TileBreeder tb = new TileBreeder();
        tb.setJobStore(store);
        tb.setThreadPoolExecutor(executor);
        tb.setTileLayerDispatcher(dispatcher);
        assertEquals(0l, store.getCount());

        job1Ready = getJob("job1.json");
        job2Running = getJob("job2.json");
        job3Done = getJob("job3.json");

        log01 = getJobLog("log01.json");
        job1Ready.addLog(log01);
        log02 = getJobLog("log02.json");
        job1Ready.addLog(log02);
        log03 = getJobLog("log03.json");
        job2Running.addLog(log03);
        log04 = getJobLog("log04.json");
        job2Running.addLog(log04);
        log05 = getJobLog("log05.json");
        job2Running.addLog(log05);
        log06 = getJobLog("log06.json");
        job2Running.addLog(log06);
        log07 = getJobLog("log07.json");
        job2Running.addLog(log07);
        log08 = getJobLog("log08.json");
        job2Running.addLog(log08);
        log09 = getJobLog("log09.json");
        job3Done.addLog(log09);
        log10 = getJobLog("log10.json");
        job3Done.addLog(log10);
        
        store.put(job1Ready);
        store.put(job2Running);
        store.put(job3Done);
        
        JobDispatcher jobDispatcher = new JobDispatcher();
        jobDispatcher.setJobStore(store);

        jobRestlet = new JobRestlet();
        jobRestlet.setXMLConfiguration(config);
        jobRestlet.setJobDispatcher(jobDispatcher);
        jobRestlet.setTileBreeder(tb);
        
        jobLogRestlet = new JobLogRestlet();
        jobLogRestlet.setXMLConfiguration(config);
        jobLogRestlet.setJobDispatcher(jobDispatcher);
    }

    @After
    public void tearDown() throws Exception {
        store.clear();
        assertEquals(0l, store.getCount());
        store.close();
        File file = new File(storePath);
        file.delete(); 
    }

    protected Representation getJSONRepresentation(String filename) {
        InputStream in = AbstractJobRestletTest.class.getResourceAsStream(filename);
        assertNotNull(in);
        return new InputRepresentation(in, MediaType.APPLICATION_JSON);
    }

    protected Representation getRepresentation(JSONObject json) {
        return new StringRepresentation(json.toString());
    }
    
    protected JobObject getJob(String filename) throws Exception {
        InputStream in = null;
        try {
            in = AbstractJobRestletTest.class.getResourceAsStream(filename);
            XStream xs = config.configureXStreamForJobs(new XStream(new DomDriver()));
            HierarchicalStreamDriver driver = new JettisonMappedXmlDriver();
            Reader reader = new InputStreamReader(in);
            HierarchicalStreamReader hsr = driver.createReader(reader);
            StringWriter writer = new StringWriter();
            new HierarchicalStreamCopier().copy(hsr, new PrettyPrintWriter(
                    writer));
            writer.close();
            String string = writer.toString();
            JobObject job = (JobObject) xs.fromXML(string);
            return job;
        } finally {
            if(in != null) {
                in.close();
            }
        }
    }
    
    protected JobLogObject getJobLog(String filename) throws Exception {
        InputStream in = null;
        try {
            in = AbstractJobRestletTest.class.getResourceAsStream(filename);
            assertNotNull(in);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            StringBuffer buff = new StringBuffer();
            String line = null;
            while((line = reader.readLine()) != null) {
                buff.append(line);
            }
            JSONObject root = new JSONObject(buff.toString());
            assertTrue(root.has("log"));
            JSONObject json = root.getJSONObject("log");
            return convertJobLog(json);
        } finally {
            if(in != null) {
                in.close();
            }
        }
    }
    
    protected JobLogObject convertJobLog(JSONObject json) throws Exception {
        JobLogObject log = new JobLogObject();
        log.setJobLogId(json.getLong("jobLogId"));
        log.setJobId(json.getLong("jobId"));
        log.setLogLevel(LOG_LEVEL.valueOf(json.getString("logLevel")));
        if(json.has("logTime")) {
            log.setLogTime(new Timestamp(getFormatter().parse(json.getString("logTime")).getTime()));
        }
        log.setLogText(json.getString("logText"));
        log.setLogSummary(json.getString("logSummary"));
        return log;
    }

    protected JSONObject getJSONJob(String filename) throws Exception {
        InputStream in = null;
        try {
            in = AbstractJobRestletTest.class.getResourceAsStream(filename);
            assertNotNull(in);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            StringBuffer buff = new StringBuffer();
            String line = reader.readLine();
            while(line != null) {
                buff.append(line);
                line = reader.readLine();
            }
            JSONObject json = new JSONObject(buff.toString());
            return json;
        } finally {
            if(in != null) {
                in.close();
            }
        }
    }

    protected void assertJobEquality(JobObject expected, JSONObject actual)
            throws Exception {

        compare(expected.getJobId(), "jobId", actual);
        compare(expected.getLayerName(), "layerName", actual);
        compare(expected.getState().name(), "state", actual);
        compare(expected.getTimeSpent(), "timeSpent", actual);
        compare(expected.getTimeRemaining(), "timeRemaining", actual);
        compare(expected.getTilesDone(), "tilesDone", actual);
        compare(expected.getTilesTotal(), "tilesTotal", actual);
        compare(expected.getFailedTileCount(), "failedTileCount", actual);
        compare(expected.getGridSetId(), "gridSetId", actual);
        compare(expected.getSrs().getNumber(), "srs", actual);
        compare(expected.getThreadCount(), "threadCount", actual);
        compare(expected.getZoomStart(), "zoomStart", actual);
        compare(expected.getZoomStop(), "zoomStop", actual);
        compare(expected.getFormat(), "format", actual);
        compare(expected.getJobType().name(), "jobType", actual);
        compare(expected.getThroughput(), "throughput", actual);
        compare(expected.getMaxThroughput(), "maxThroughput", actual);
        compare(expected.getPriority().name(), "priority", actual);
        compare(expected.getSchedule(), "schedule", actual);
        compare(expected.getSpawnedBy(), "spawnedBy", actual);
        compare(expected.isRunOnce(), "runOnce", actual);
        compare(expected.isFilterUpdate(), "filterUpdate", actual);
        compare(expected.getEncodedParameters(), "encodedParameters", actual);
        compare(expected.getTimeFirstStart(), "timeFirstStart", actual);
        compare(expected.getTimeLatestStart(), "timeLatestStart", actual);
        compare(expected.getTimeFinish(), "timeFinish", actual);
    }
    
    private static double FP_TOLERANCE = 10e-5;
    private static long DT_TOLERANCE = 1;
    private static TimeUnit DT_TOLERANCE_UNITS = TimeUnit.SECONDS;
    
    private void compare(float expected, String jsonKey, JSONObject json) throws Exception {
        if(expected != -1) {
            assertTrue("Expected value for " + jsonKey, json.has(jsonKey));
            float jsonFloat = Double.valueOf(json.getDouble(jsonKey)).floatValue();
            assertEquals("Expected " + jsonKey + " of " + expected + " but found " + jsonFloat,
                    expected, jsonFloat, FP_TOLERANCE);
        } else {
            // 0 is our 'not set' value, so in this case it can be missing or equal
            if(json.has(jsonKey)) {
                float jsonFloat = Double.valueOf(json.getDouble(jsonKey)).floatValue();
                assertEquals("Expected " + jsonKey + " of " + expected + " but found " + jsonFloat,
                        expected, jsonFloat, FP_TOLERANCE);
            }
        }
    }
    
    private void compare(boolean expected, String jsonKey, JSONObject json) throws Exception {
        if(expected) {
            assertTrue("Expected value for " + jsonKey, json.has(jsonKey));
            boolean jsonBool = json.getBoolean(jsonKey);
            assertEquals("Expected " + jsonKey + " of " + expected + " but found " + jsonBool,
                    expected, jsonBool);
        } else {
            // The default value is false, so we can't tell if it has been set false, or not set
            if(json.has(jsonKey)) {
                assertTrue("Expected value for " + jsonKey, json.has(jsonKey));
                boolean jsonBool = json.getBoolean(jsonKey);
                assertEquals("Expected " + jsonKey + " of " + expected + " but found " + jsonBool,
                        expected, jsonBool);

            }
        }
    }

    private void compare(Timestamp expected, String jsonKey, JSONObject json) throws Exception {
        if(expected != null) {
            assertTrue("Expected value for "  + jsonKey, json.has(jsonKey));
            long expectedTime = expected.getTime();
            Date actualDate = getFormatter().parse(json.getString(jsonKey));
            long actualTime = actualDate.getTime();
            long toleranceMillies = TimeUnit.MILLISECONDS.convert(DT_TOLERANCE, DT_TOLERANCE_UNITS);
            long difference = expectedTime >= actualTime
                    ? expectedTime - actualTime : actualTime - expectedTime;
            long differenceInputUnits = DT_TOLERANCE_UNITS.convert(difference, TimeUnit.MILLISECONDS);
            assertTrue("Expected " + jsonKey + " value [" + getFormatter().format(expected) +
                    "] to differ from actual [" + getFormatter().format(actualDate) + "] by at most " +
                    DT_TOLERANCE + " " + DT_TOLERANCE_UNITS.name() + " but was " + differenceInputUnits, 
                    difference <= toleranceMillies);
        } else {
            assertFalse("Expected no value for " + jsonKey, json.has(jsonKey));
        }
    }
    
    public static String findTempDir() throws Exception {
        String tmpDir = System.getProperty("java.io.tmpdir");
        if(tmpDir == null || ! (new File(tmpDir)).canWrite()) {
            throw new Exception("Temporary directory " 
                    + tmpDir + " does not exist or is not writable.");
        }
        return tmpDir;
    }
    
    private void compare(String expected, String jsonKey, JSONObject json) throws Exception {
        if(expected != null) {
            assertTrue("Expected value for " + jsonKey, json.has(jsonKey));
            String jsonString = json.getString(jsonKey);
            assertEquals("Expected " + jsonKey + " of " + expected + " but found " + jsonString,
                    expected, jsonString);
        } else {
            assertFalse("Expected no value for " + jsonKey, json.has(jsonKey));
        }
    }
    
    private void compare(long expected, String jsonKey, JSONObject json) throws Exception {
        if(expected != -1) {
            assertTrue("Expected value for " + jsonKey, json.has(jsonKey));
            long jsonLong = json.getLong(jsonKey);
            assertEquals("Expected " + jsonKey + " of " + expected + " but found " + jsonLong,
                    expected, jsonLong);
        } else {
            // -1 is our pseudo-null value, so it's fine if it's missing, but must match if it's there
            if(json.has(jsonKey)) {
                long jsonLong = json.getLong(jsonKey);
                assertEquals("Expected " + jsonKey + " of " + expected + " but found " + jsonLong,
                        expected, jsonLong);
            }
        }
    }
    
    private void compare(int expected, String jsonKey, JSONObject json) throws Exception {
        if(expected != -1) {
            assertTrue("Expected value for " + jsonKey, json.has(jsonKey));
            int jsonInt = json.getInt(jsonKey);
            assertEquals("Expected " + jsonKey + " of " + expected + " but found " + jsonInt,
                    expected, jsonInt);
        } else {
            // -1 is our pseudo-null value, so it's fine if it's missing, but must match if it's there
            if(json.has(jsonKey)) {
                int jsonInt = json.getInt(jsonKey);
                assertEquals("Expected " + jsonKey + " of " + expected + " but found " + jsonInt,
                        expected, jsonInt);
            }
        }
    }

    DateFormat formatter = null;

    private DateFormat getFormatter() {
        if(formatter == null) {
            formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        } 
        return formatter;
    }



}
