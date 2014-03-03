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

import java.sql.Timestamp;

import org.geowebcache.storage.JobLogObject;
import org.geowebcache.storage.JobLogObject.LOG_LEVEL;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.bind.tuple.TupleTupleBinding;


/**
 * A TupleTupleBinding implementation for en/decoding a JobLogObject.
 * 
 * All primitive and simple object encoding is handled by the parent write functions, while
 * enums are encoded as their string names.
 * The order of encoding is:
 * <ol>
 * <li>jobLogId</li>
 * <li>jobId</li>
 * <li>logLevel</li>
 * <li>logSummary</li>
 * <li>logText</li>
 * </ol>
 * 
 * @author mleslie
 */
public class JobLogObjectEntityBinding extends TupleTupleBinding<JobLogObject> {
    private JobLogObjectKeyBinding keyBinding;
    
    public JobLogObjectEntityBinding(JobLogObjectKeyBinding keyBinding) {
        this.keyBinding = keyBinding;
    }
    
    public JobLogObjectEntityBinding() {
        this.keyBinding = new JobLogObjectKeyBinding();
    }

    @Override
    public JobLogObject entryToObject(TupleInput keyInput, TupleInput dataInput) {
        JobLogObject log = new JobLogObject();
        log.setJobLogId(keyBinding.entryToObject(keyInput));
        
        log.setJobId(dataInput.readPackedLong());
        log.setLogLevel(LOG_LEVEL.valueOf(dataInput.readString()));
        log.setLogTime(new Timestamp(dataInput.readLong()));
        log.setLogSummary(dataInput.readString());
        log.setLogText(dataInput.readString());
        
        return log;
    }

    @Override
    public void objectToKey(JobLogObject object, TupleOutput output) {
        keyBinding.objectToEntry(object.getJobLogId(), output);
    }

    @Override
    public void objectToData(JobLogObject log, TupleOutput output) {
        output.writePackedLong(log.getJobId());
        output.writeString(log.getLogLevel().name());
        output.writeLong(log.getLogTime().getTime());
        output.writeString(log.getLogSummary());
        output.writeString(log.getLogText());
    }

}
