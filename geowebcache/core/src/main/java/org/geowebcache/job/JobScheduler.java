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
package org.geowebcache.job;

import it.sauronsoftware.cron4j.InvalidPatternException;
import it.sauronsoftware.cron4j.Scheduler;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.seed.ScheduledJobInitiator;
import org.geowebcache.seed.TileBreeder;
import org.geowebcache.storage.JobLogObject;
import org.geowebcache.storage.JobObject;
import org.geowebcache.storage.JobStore;
import org.geowebcache.storage.StorageException;


/**
 * Synchronises all access to a singleton cron4j Scheduler.
 */
public class JobScheduler {

    private static Log log = LogFactory.getLog(JobScheduler.class);

    private static Scheduler instance;

    /**
     * The job scheduler keeps track of the cron4j schedule ID's and maps them to jobs.
     */
    private static Map<Long, String> scheduleIds;

    static {
        instance = new Scheduler();
        instance.setDaemon(true);
        instance.start();
        scheduleIds = new HashMap<Long, String>();
    }

    public static void scheduleJob(JobObject job, TileBreeder seeder, JobStore jobStore) {
        // scheduled jobs don't run immediately, but the job monitor needs to be aware of them.
        ScheduledJobInitiator sji = new ScheduledJobInitiator(job, seeder, jobStore);
        synchronized(instance) {
            try {
                scheduleIds.put(job.getJobId(), instance.schedule(job.getSchedule(), sji));
                log.info("Job " + job.getJobId() + " has been scheduled.");
            } catch (InvalidPatternException e) {
                log.error("Couldn't schedule job " + job.getJobId() + " - invalid schedule pattern: '" + job.getSchedule() + "'.");
                job.addLog(JobLogObject.createErrorLog(job.getJobId(), "Couldn't schedule job", "Job has an invalid schedule pattern: '" + job.getSchedule() + "'."));
                try {
                    jobStore.put(job);
                } catch (StorageException se) {
                    log.error("Couldn't save job log.", se);
                }
            }
        }
    }

    public static void deschedule(long jobId) {
        String scheduleId = null;
        synchronized(instance) {
            if(scheduleIds.containsKey(jobId)) {
                scheduleId = scheduleIds.get(jobId);
            }
        }

        if(scheduleId != null) {
            synchronized(instance) {
                instance.deschedule(scheduleId);
                if(scheduleIds.containsValue(scheduleId)) {
                    scheduleIds.remove(jobId);
                    log.info("Job " + jobId + " has been de-scheduled.");
                }
            }
        }
    }

    public static Scheduler getSchedulerInstance() {
        return instance;
    }

    private JobScheduler() {
        ;
    }
}
