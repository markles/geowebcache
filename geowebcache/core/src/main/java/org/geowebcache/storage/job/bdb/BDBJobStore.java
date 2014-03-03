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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.config.ConfigurationException;
import org.geowebcache.seed.GWCTask.STATE;
import org.geowebcache.storage.DefaultStorageFinder;
import org.geowebcache.storage.JobLogObject;
import org.geowebcache.storage.JobObject;
import org.geowebcache.storage.JobStore;
import org.geowebcache.storage.StorageException;
import org.geowebcache.storage.job.bdb.bind.JobLogObjectEntityBinding;
import org.geowebcache.storage.job.bdb.bind.JobLogObjectKeyBinding;
import org.geowebcache.storage.job.bdb.bind.JobObjectEntityBinding;
import org.geowebcache.storage.job.bdb.bind.JobObjectKeyBinding;
import org.geowebcache.storage.job.bdb.bind.StateKeyBinding;
import org.geowebcache.storage.job.bdb.bind.TimeKeyBinding;
import org.springframework.util.Assert;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;

/**
 * This JobStore is responsible for all access to the underlying BDB-je datastore that persists
 * job and log details.
 * 
 * @author mleslie
 */
public class BDBJobStore implements JobStore {

private static final Log log = LogFactory.getLog(BDBJobStore.class);

private String cacheRootDir;
private long clearOldJobsSetting = -1;

private JobStoreIndexer indexer;

public BDBJobStore(final DefaultStorageFinder cacheDirFinder) 
        throws ConfigurationException {
    Assert.notNull(cacheDirFinder, "cacheDirFinder can't be null");
    
    this.cacheRootDir = cacheDirFinder.getDefaultPath();
    
    startUp();
}

public BDBJobStore(String cacheRootDir) {
    this.cacheRootDir = cacheRootDir;
    
    startUp();
}

private boolean open;

public void startUp() {
    File storeDirectory = new File(cacheRootDir, "job_store");
    storeDirectory.mkdirs();
    
    JobStoreConfig config = new JobStoreConfig();
    indexer = new JobStoreIndexer(config);
    
    indexer.init(storeDirectory);
   
    open = true;
}

public void close() throws StorageException {
    open = false;
    clearId();
    clearLogId();
    indexer.close();
    log.info("Job store closed");
}

public boolean delete(long jobId) throws StorageException {
    if(!open) {
        throw new StorageException("Job store has not been started or has been closed.");
    }
    JobObjectEntityBinding binding = new JobObjectEntityBinding();
//    JobIdBinding jobIdBinding = new JobIdBinding();
    JobObjectKeyBinding jobIdBinding = new JobObjectKeyBinding();
    
    JobObject job = new JobObject();
    job.setJobId(jobId);
    Cursor cursor = null;
    SecondaryCursor sCursor = null;
    try {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        DatabaseEntry searchKey = new DatabaseEntry();
        
        sCursor = indexer.getJobLogByJobDatabase().openCursor(null, null);
        jobIdBinding.objectToEntry(jobId, searchKey);
        OperationStatus ret = sCursor.getSearchKey(searchKey, key, data, LockMode.DEFAULT);
        while(ret == OperationStatus.SUCCESS) {
            sCursor.delete();
            ret = sCursor.getNextDup(searchKey, key, data, LockMode.DEFAULT);
        }
        
        binding.objectToKey(job, key);
        cursor = indexer.getJobDatabase().openCursor(null, null);
        ret = cursor.getSearchKey(key, data, LockMode.DEFAULT);
        if(ret == OperationStatus.SUCCESS) {
            ret = cursor.delete();
            if(ret == OperationStatus.SUCCESS) {
                return true;
            }
        }
        return false;
    } finally {
        if(cursor != null) {
            cursor.close();
        }
        if(sCursor != null) {
            sCursor.close();
        }
    }
}

public boolean get(JobObject job) throws StorageException {
    if(!open) {
        throw new StorageException("Job store has not been started or has been closed.");
    }
    JobObjectEntityBinding binding = new JobObjectEntityBinding();
    Cursor cursor = null;
    try {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        binding.objectToKey(job, key);
        cursor = indexer.getJobDatabase().openCursor(null, null);
        OperationStatus ret = cursor.getSearchKey(key, data, LockMode.DEFAULT);
        if(ret == OperationStatus.SUCCESS) {
            JobObject newJob = binding.entryToObject(key, data);
            copyJobInternals(newJob, job);
            return true;
        } else {
            return false;
        }
    } finally {
        if(cursor != null) {
            cursor.close();
        }
    }
}

private void copyJobInternals(JobObject sourceJob, JobObject targetJob) {
    if(sourceJob == null || targetJob == null || sourceJob.getJobId() != targetJob.getJobId()) {
        return;
    }
    targetJob.setLayerName(sourceJob.getLayerName());
    targetJob.setState(sourceJob.getState());
    targetJob.setTimeSpent(sourceJob.getTimeSpent());
    targetJob.setTimeRemaining(sourceJob.getTimeRemaining());
    targetJob.setTilesDone(sourceJob.getTilesDone());
    targetJob.setTilesTotal(sourceJob.getTilesTotal());
    targetJob.setFailedTileCount(sourceJob.getFailedTileCount());
    
    targetJob.setBounds(sourceJob.getBounds());
    targetJob.setGridSetId(sourceJob.getGridSetId());
    targetJob.setSrs(sourceJob.getSrs());
    targetJob.setThreadCount(sourceJob.getThreadCount());
    targetJob.setZoomStart(sourceJob.getZoomStart());
    targetJob.setZoomStop(sourceJob.getZoomStop());
    targetJob.setFormat(sourceJob.getFormat());
    targetJob.setJobType(sourceJob.getJobType());
    targetJob.setThroughput(sourceJob.getThroughput());
    targetJob.setMaxThroughput(sourceJob.getMaxThroughput());
    
    targetJob.setPriority(sourceJob.getPriority());
    targetJob.setSchedule(sourceJob.getSchedule());
    targetJob.setSpawnedBy(sourceJob.getSpawnedBy());
    targetJob.setRunOnce(sourceJob.isRunOnce());
    targetJob.setFilterUpdate(sourceJob.isFilterUpdate());
    targetJob.setEncodedParameters(sourceJob.getEncodedParameters());
    
    targetJob.setTimeFirstStart(sourceJob.getTimeFirstStart());
    targetJob.setTimeLatestStart(sourceJob.getTimeLatestStart());
    targetJob.setTimeFinish(sourceJob.getTimeFinish());
    
    targetJob.setWarnCount(sourceJob.getWarnCount());
    targetJob.setErrorCount(sourceJob.getErrorCount());
}


public void put(JobObject job) throws StorageException {
    if(!open) {
        throw new StorageException("Job store has not been started or has been closed.");
    }
    if(job == null) {
        throw new StorageException("Error writing JobObject to datastore.", 
                new IllegalArgumentException("Cannot persist null job."));
    }
    Cursor cursor = null;
    try {
        Iterator<JobLogObject> it = job.getNewLogs().iterator();
        while(it.hasNext()) {
            JobLogObject log = it.next();
            put(log);
        }
        if(job.getJobId() == -1) {
            job.setJobId(getNextId());
        }
        JobObjectEntityBinding binding = new JobObjectEntityBinding();
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        binding.objectToKey(job, key);
        binding.objectToData(job, data);
        
        cursor = indexer.getJobDatabase().openCursor(null, null);
        OperationStatus ret = cursor.put(key, data);
        if(ret != OperationStatus.SUCCESS) {
            log.warn("Storage of job " + job.getJobId() + 
                    " was not successful with a status of " + ret.name());
        }
            
    } catch(IllegalArgumentException ex) {
        log.error("Error writing JobObject to datastore.", ex);
        throw new StorageException("Error writing JobObject to datastore.", ex);
    } catch(DatabaseException ex) {
        log.error("Error writing JobObject to datastore.", ex);
        throw new StorageException("Error writing JobObject ot datastore.", ex);
    } finally {
        if(cursor != null) {
            cursor.close();
        }
    }
}

private void put(JobLogObject log) throws StorageException {
    JobLogObjectEntityBinding binding = new JobLogObjectEntityBinding();
    Cursor cursor = null;
    try {
        if(log.getJobLogId() == -1) {
            log.setJobLogId(getNextLogId());
        }
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        binding.objectToKey(log, key);
        binding.objectToData(log, data);
        
        cursor = indexer.getJobLogDatabase().openCursor(null, null);
        
        OperationStatus ret = cursor.put(key, data);
        if(ret != OperationStatus.SUCCESS) {
            BDBJobStore.log.warn("Storage of log " + log.getJobLogId() + " for job " + log.getJobId() +
                    " was not successful with a status of " + ret.name());
        }        
    } finally {
        if(cursor != null) {
            cursor.close();
        }
    }
}

public long getCount() throws StorageException {
    if(!open) {
        throw new StorageException("Job store has not been started or has been closed.");
    }
    return indexer.getJobDatabase().count();
}

public Iterable<JobObject> getJobs() throws StorageException {
    if(!open) {
        throw new StorageException("Job store has not been started or has been closed.");
    }
    JobObjectEntityBinding binding = new JobObjectEntityBinding();
    List<JobObject> results = new ArrayList<JobObject>();
    Cursor cursor = null;
    try {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        
        cursor = indexer.getJobDatabase().openCursor(null, null);
        
        while(OperationStatus.SUCCESS == cursor.getNext(key, data, LockMode.DEFAULT)) {
            JobObject job = binding.entryToObject(key, data);
            results.add(job);
        }
        return results;
    } finally {
        if(cursor != null) {
            cursor.close();
        }
    }
}

/*
 * UNSET and READY
 */
public Iterable<JobObject> getPendingScheduledJobs() throws StorageException {
    if(!open) {
        throw new StorageException("Job store has not been started or has been closed.");
    }
    List<JobObject> results = new ArrayList<JobObject>();
    getJobsByState(STATE.READY, results);
    getJobsByState(STATE.UNSET, results);
    
    return results;
}

public Iterable<JobObject> getInterruptedJobs() throws StorageException {
    if(!open) {
        throw new StorageException("Job store has not been started or has been closed.");
    }
    List<JobObject> results = new ArrayList<JobObject>();
    getJobsByState(STATE.INTERRUPTED, results);
    getJobsByState(STATE.DEAD, results);
    
    return results;
}

private void getJobsByState(STATE state, List<JobObject> results) {
    if(results == null) {
        throw new IllegalArgumentException("Result list cannot be null.");
    }
    DatabaseEntry keyEntry = new DatabaseEntry();
    StateKeyBinding keyBinder = new StateKeyBinding();
    JobObjectEntityBinding binder = new JobObjectEntityBinding();
    
    keyBinder.objectToEntry(state, keyEntry);
    SecondaryCursor cursor = null;
    try {
        cursor = indexer.getJobByStateDatabase().openCursor(null, null);
        DatabaseEntry pk = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        OperationStatus ret = cursor.getSearchKey(keyEntry, pk, data, LockMode.DEFAULT);
        while(ret == OperationStatus.SUCCESS) {
            JobObject job = binder.entryToObject(pk, data);
            results.add(job);
            ret = cursor.getNextDup(keyEntry, pk, data, LockMode.DEFAULT);
        }
    } finally {
        if(cursor != null) {
            cursor.close();
        }
    }
}

public Iterable<JobLogObject> getLogs(long jobId) throws StorageException {
    if(!open) {
        throw new StorageException("Job store has not been started or has been closed.");
    }
    List<JobLogObject> result = new ArrayList<JobLogObject>();
    JobLogObjectEntityBinding binding = new JobLogObjectEntityBinding();
//    JobIdBinding keyBinder = new JobIdBinding();
    JobObjectKeyBinding keyBinder = new JobObjectKeyBinding();
    DatabaseEntry key = new DatabaseEntry();
    keyBinder.objectToEntry(jobId, key);
    SecondaryCursor cursor = null;
    try {
        DatabaseEntry pk = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        
        cursor = indexer.getJobLogByJobDatabase().openCursor(null, null);
        
        OperationStatus ret = cursor.getSearchKey(key, pk, data, LockMode.DEFAULT);
        while(ret == OperationStatus.SUCCESS) { 
            JobLogObject log = binding.entryToObject(pk, data);
            result.add(log);
            ret = cursor.getNextDup(key, pk, data, LockMode.DEFAULT);
        }
    } finally {
        if(cursor != null) {
            cursor.close();
        }
    }
    return result;
    
}

public Iterable<JobLogObject> getAllLogs() throws StorageException {
    if(!open) {
        throw new StorageException("Job store has not been started or has been closed.");
    }
    List<JobLogObject> result = new ArrayList<JobLogObject>();
    JobLogObjectEntityBinding binding = new JobLogObjectEntityBinding();
    Cursor cursor = null;
    try {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        cursor = indexer.getJobLogDatabase().openCursor(null, null);
        OperationStatus ret = cursor.getNext(key, data, LockMode.DEFAULT);
        while(ret == OperationStatus.SUCCESS) {
            JobLogObject log = binding.entryToObject(key, data);
            result.add(log);
            ret = cursor.getNext(key, data, LockMode.DEFAULT);
        }
    } finally {
        if(cursor != null) {
            cursor.close();
        }
    }
    return result;
}

public long getClearOldJobsSetting() throws StorageException {
    return clearOldJobsSetting;
}

public void setClearOldJobsSetting(long clearOldJobsVal)
        throws StorageException {
    this.clearOldJobsSetting = clearOldJobsVal;
}

public long purgeOldJobs(Timestamp ts) throws StorageException {
    if(!open) {
        throw new StorageException("Job store has not been started or has been closed.");
    }
    long count = 0;
    JobObjectEntityBinding binding = new JobObjectEntityBinding();
    TimeKeyBinding timeBinding = new TimeKeyBinding();
    DatabaseEntry keyMin = new DatabaseEntry();
    timeBinding.objectToEntry(new Timestamp(0l), keyMin);
    SecondaryCursor cursor = null;
    try {
        DatabaseEntry pk = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        cursor = indexer.getJobByTimeDatabase().openCursor(null, null);
        OperationStatus ret = cursor.getSearchKeyRange(keyMin, pk, data, LockMode.DEFAULT);
        while(ret == OperationStatus.SUCCESS) {
            JobObject job = binding.entryToObject(pk, data);
            if(job.getTimeFinish().compareTo(ts) <= 0) {
                ret = cursor.delete();
                if(ret == OperationStatus.SUCCESS) {
                    count++;
                }
            } else {
                break;
            }
            ret = cursor.getSearchKeyRange(keyMin, pk, data, LockMode.DEFAULT);
        }
        return count;
    } finally {
        if(cursor != null) {
            cursor.close();
        }
    }
}

public void clear() throws StorageException {
    if(!open) {
        throw new StorageException("Job store has not been started or has been closed.");
    }
    Cursor jobCursor = null;
    DatabaseEntry key = new DatabaseEntry();
    DatabaseEntry data = new DatabaseEntry();
    try {
        jobCursor = indexer.getJobDatabase().openCursor(null, null);
        OperationStatus ret = jobCursor.getNext(key, data, LockMode.DEFAULT);
        while(OperationStatus.SUCCESS == ret) {
            ret = jobCursor.delete();
            jobCursor.getNext(key, data, LockMode.DEFAULT);
        }
    } finally {
        if(jobCursor != null) {
            jobCursor.close();
        }
    }
    Cursor logCursor = null;
    try {
        logCursor = indexer.getJobLogDatabase().openCursor(null,  null);
        OperationStatus ret = logCursor.getNext(key, data, LockMode.DEFAULT);
        while(OperationStatus.SUCCESS == ret) {
            ret = logCursor.delete();
            logCursor.getNext(key, data, LockMode.DEFAULT);
        }
    } finally {
        if(logCursor != null) {
            logCursor.close();
        }
    }
}

/**
 * This is a spring-friendly destroy method.  If we're failing here, we really want
 * spring to be able to continue cleaning up.  For now, I'm going to eat this exception
 * to let that happen.
 */
public void destroy() {
    try {
        close();
    } catch(StorageException ex) {
        log.error("Error attempting to shut down and destroy the job store.", ex);
    }
}

private static Long lastId;
private long getNextId() {
    synchronized(BDBJobStore.class) {
        if(lastId == null) {
            lastId = new Long(0);
            Cursor cursor = null;
            JobObjectKeyBinding binding = new JobObjectKeyBinding();
            try {
                DatabaseEntry keyEntry = new DatabaseEntry();
                DatabaseEntry dataEntry = new DatabaseEntry();
                cursor = indexer.getJobDatabase().openCursor(null, null);
                while(cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                    Long currKey = binding.entryToObject(keyEntry);
                    if(currKey > lastId) {
                        lastId = currKey;
                    }
                }
            } finally {
                if(cursor != null) {
                    cursor .close();
                }
            }
        }
        lastId = new Long(lastId + 1);
        return lastId;
    }
}

private static Long lastLogId;
private long getNextLogId() {
    synchronized(BDBJobStore.class) {
        if(lastLogId == null) {
            lastLogId = new Long(0);
            Cursor cursor = null;
            JobLogObjectKeyBinding binding = new JobLogObjectKeyBinding();
            try {
                DatabaseEntry keyEntry = new DatabaseEntry();
                DatabaseEntry dataEntry = new DatabaseEntry();
                cursor = indexer.getJobLogDatabase().openCursor(null, null);
                while(cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                    Long currKey = binding.entryToObject(keyEntry);
                    if(currKey > lastLogId) {
                        lastLogId = currKey;
                    }
                }
            } finally {
                if(cursor != null) {
                    cursor.close();
                }
            }
        }
        lastLogId = new Long(lastLogId + 1);
        return lastLogId;
    }
}

private void clearId() {
    synchronized(BDBJobStore.class) {
        lastId = null;
    }
}

private void clearLogId() {
    synchronized(BDBJobStore.class) {
        lastLogId = null;
    }
}

}
