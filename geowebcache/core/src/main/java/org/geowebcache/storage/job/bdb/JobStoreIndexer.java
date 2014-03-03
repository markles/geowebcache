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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.storage.JobLogObject;
import org.geowebcache.storage.JobObject;
import org.geowebcache.storage.job.bdb.bind.JobLogObjectEntityBinding;
import org.geowebcache.storage.job.bdb.bind.JobObjectEntityBinding;
import org.geowebcache.storage.job.bdb.bind.JobObjectKeyBinding;
import org.geowebcache.storage.job.bdb.bind.StateKeyBinding;
import org.geowebcache.storage.job.bdb.bind.TimeKeyBinding;

import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

/**
 * Responsible for setting up and maintaining the Berkeley databases used by the BDBJobStore.
 * 
 * @author mleslie
 */
public class JobStoreIndexer {

    private static final Log log = LogFactory.getLog(JobStoreIndexer.class);
    
    private JobStoreConfig config;

    public JobStoreIndexer(JobStoreConfig config) {
        this.config = config;
    }
    
    private Environment env;
    
    private Database jobDatabase;
    private SecondaryDatabase jobByStateDatabase;
    private SecondaryDatabase jobByTimeDatabase;

    private Database jobLogDatabase;
    private SecondaryDatabase jobLogByJobDatabase;
    
    /**
     * 
     * @param storeDirectory
     * @param bdbEnvProperties properties for the {@link EnvironmentConfig}, or {@code null}. If not
     *        provided {@code environment.properties} will be looked up for inside
     *        {@code storeDirectory}
     * @return
     */
    public void init(final File storeDirectory) {

        EnvironmentConfig envCfg = new EnvironmentConfig();
        envCfg.setAllowCreate(true);
        envCfg.setCacheMode(CacheMode.DEFAULT);
        envCfg.setLockTimeout(1000, TimeUnit.MILLISECONDS);
        envCfg.setDurability(Durability.COMMIT_WRITE_NO_SYNC);
        envCfg.setSharedCache(true);
        envCfg.setTransactional(true);
        envCfg.setConfigParam("je.log.fileMax", String.valueOf(100 * 1024 * 1024));

        if (config.getCacheMemoryPercentAllowed() == null) {
            if (config.getCacheSizeMB() == null) {
                log.info("Neither disk quota page store' cache memory percent nor cache size was provided."
                        + " Defaulting to 25% Heap Size");
                envCfg.setCachePercent(25);
            } else {
                log.info("Disk quota page store cache explicitly set to " + config.getCacheSizeMB() + "MB");
                envCfg.setCacheSize(config.getCacheSizeMB());
            }
        } else {
            envCfg.setCachePercent(config.getCacheMemoryPercentAllowed());
        }

        env = new Environment(storeDirectory, envCfg);
        
        /*
         * Job data store bits
         */
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        jobDatabase = env.openDatabase(null, "GWC Job Store", dbConfig);
        
        JobObjectEntityBinding binder = new JobObjectEntityBinding();
        
        SecondaryConfig sdbConfig = new SecondaryConfig();
        sdbConfig.setAllowCreate(true);
        sdbConfig.setSortedDuplicates(true);
        sdbConfig.setKeyCreator(new StateKeyGenerator(binder));
        
        jobByStateDatabase = env.openSecondaryDatabase(null, "GWC Job Store by State", 
                jobDatabase, sdbConfig);
        
        sdbConfig.setKeyCreator(new TimeKeyGenerator(binder));
        
        jobByTimeDatabase = env.openSecondaryDatabase(null, "GWC Job Store by time", 
                jobDatabase, sdbConfig);
        
        /*
         * Job Log data store bits
         */
        jobLogDatabase = env.openDatabase(null, "GWC Job Log Store", dbConfig);
        
        sdbConfig.setKeyCreator(new JobLogJobKeyGenerator());
        jobLogByJobDatabase = env.openSecondaryDatabase(null, "GWC Job Log Store by Job", 
                jobLogDatabase, sdbConfig);
    }
    
    public void close() {
        jobByStateDatabase.close();
        jobByTimeDatabase.close();
        jobDatabase.close();
        
        jobLogByJobDatabase.close();
        jobLogDatabase.close();
        env.close();
    }
    
    public Database getJobDatabase() {
        return jobDatabase;
    }
    
    public SecondaryDatabase getJobByStateDatabase() {
        return jobByStateDatabase;
    }
    
    public SecondaryDatabase getJobByTimeDatabase() {
        return jobByTimeDatabase;
    }
    
    public Database getJobLogDatabase() {
        return jobLogDatabase;
    }
    
    public SecondaryDatabase getJobLogByJobDatabase() {
        return jobLogByJobDatabase;
    }
    
    /**
     * Generates a secondary key for a JobLogObject database using the jobId.
     * Delegates the encoding to JobObjectKeyBinding for consistency.
     */
    private class JobLogJobKeyGenerator implements SecondaryKeyCreator {
        private JobLogObjectEntityBinding binding;
        private JobObjectKeyBinding jobIdBinding;
        
        public JobLogJobKeyGenerator() {
            binding = new JobLogObjectEntityBinding();
            jobIdBinding = new JobObjectKeyBinding();
        }

        public boolean createSecondaryKey(SecondaryDatabase secondary,
                DatabaseEntry key, DatabaseEntry data, DatabaseEntry result) {

            try {
                JobLogObject log = binding.entryToObject(key, data);
                jobIdBinding.objectToEntry(log.getJobId(), result);
                return true;
            } catch(Exception ex) {
                log.error("Secondary key creation failed.  Data will not be indexed.", ex);
                return false;
            }
        }
        
    }
    
    /**
     * Generates a secondary key for a JobObject database using the state
     * field.  Delegates the encoding to StateKeyBinding for consistency.
     */
    private class StateKeyGenerator extends AbstractJobKeyGenerator {
        private StateKeyBinding stateBinding;
        public StateKeyGenerator(JobObjectEntityBinding binding) {
            super(binding);
            this.stateBinding = new StateKeyBinding();
        }

        @Override
        protected boolean extractKey(JobObject job, DatabaseEntry result) throws IOException {
            stateBinding.objectToEntry(job.getState(), result);
            return true;
        }
    }
    
    /**
     * Generates a secondary key for a JobObject database using the timeFinish
     * field.  Delegates the encoding to TimeKeyBinding for consistency.
     */
    private class TimeKeyGenerator extends AbstractJobKeyGenerator {
        private TimeKeyBinding timeBinding;
    public TimeKeyGenerator(JobObjectEntityBinding binding) {
        super(binding);
        this.timeBinding = new TimeKeyBinding();
    }

    @Override
    protected boolean extractKey(JobObject job, DatabaseEntry result) throws IOException {
        if(job.getTimeFinish() == null) {
            return false;
        }
        this.timeBinding.objectToEntry(job.getTimeFinish(), result);
        return true;
    }

    }

}
