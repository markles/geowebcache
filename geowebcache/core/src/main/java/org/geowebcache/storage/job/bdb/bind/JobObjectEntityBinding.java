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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.grid.BoundingBox;
import org.geowebcache.grid.SRS;
import org.geowebcache.seed.GWCTask.PRIORITY;
import org.geowebcache.seed.GWCTask.STATE;
import org.geowebcache.seed.GWCTask.TYPE;
import org.geowebcache.storage.JobObject;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.bind.tuple.TupleTupleBinding;

/**
 * A TupleTupleBinding implementation for en/decoding a JobObject.
 * 
 * All primitive and simple object encoding is handled by the parent write functions, while
 * enums are encoded as their string names.  Timestamp fields are encoded as longs and any
 * field that is expected to be null is preceded by a boolean that indicates if an encoded field
 * The bounds are encoded as a sequence of double values (minx, miny, maxx, maxy).  SRS is encoded
 * as the SRS name.
 * follows or was null and thus not encoded.
 * The order of encoding is:
 * <ol>
 * <li>jobId</li>
 * <li>layerName</li>
 * <li>state</li>
 * <li>timeSpent</li>
 * <li>timeRemaining</li>
 * <li>tilesDone</li>
 * <li>tilesTotal</li>
 * <li>failedTileCount</li>
 * <li>gridSetId</li>
 * <li>threadCount</li>
 * <li>zoomStart</li>
 * <li>zoomStop</li>
 * <li>format</li>
 * <li>jobType</li>
 * <li>throughput</li>
 * <li>maxThroughput</li>
 * <li>priority</li>
 * <li>schedule</li>
 * <li>spawnedBy</li>
 * <li>runOnce</li>
 * <li>filterUpdate</li>
 * <li>encodedParameters</li>
 * <li>warnCount</li>
 * <li>errorCount</li>
 * <li>timeFirstStart</li>
 * <li>timeLatestStart</li>
 * <li>timeFinish</li>
 * <li>bounds</li>
 * <li>srs</li>
 * </ol>
 * 
 * @author mleslie
 */
public class JobObjectEntityBinding extends TupleTupleBinding<JobObject> {
    private static Log log = LogFactory.getLog(JobObjectEntityBinding.class);
    private JobObjectKeyBinding keyBinder;

    public JobObjectEntityBinding() {
        keyBinder = new JobObjectKeyBinding();
    }
    
    @Override
    public JobObject entryToObject(TupleInput keyInput, TupleInput dataInput) {
        JobObject job = new JobObject();
        job.setJobId(keyBinder.entryToObject(keyInput));
        job.setLayerName(dataInput.readString());
        job.setState(STATE.valueOf(dataInput.readString()));
        job.setTimeSpent(dataInput.readLong());
        job.setTimeRemaining(dataInput.readLong());
        job.setTilesDone(dataInput.readLong());
        job.setTilesTotal(dataInput.readLong());
        job.setFailedTileCount(dataInput.readLong());
        job.setGridSetId(dataInput.readString());
        job.setThreadCount(dataInput.readInt());
        job.setZoomStart(dataInput.readInt());
        job.setZoomStop(dataInput.readInt());
        job.setFormat(dataInput.readString());
        job.setJobType(TYPE.valueOf(dataInput.readString()));
        job.setThroughput(dataInput.readFloat());
        job.setMaxThroughput(dataInput.readInt());
        job.setPriority(PRIORITY.valueOf(dataInput.readString()));
        job.setSchedule(dataInput.readString());
        job.setSpawnedBy(dataInput.readLong());
        job.setRunOnce(dataInput.readBoolean());
        job.setFilterUpdate(dataInput.readBoolean());
        job.setEncodedParameters(dataInput.readString());
        job.setWarnCount(dataInput.readLong());
        job.setErrorCount(dataInput.readLong());
        
        boolean next = dataInput.readBoolean();
        if(next) {
            job.setTimeFirstStart(new Timestamp(dataInput.readLong()));
        }
        next = dataInput.readBoolean();
        if(next) {
            job.setTimeLatestStart(new Timestamp(dataInput.readLong()));
        }
        next = dataInput.readBoolean();
        if(next) {
            job.setTimeFinish(new Timestamp(dataInput.readLong()));
        }
        
        next = dataInput.readBoolean();
        if(next) {
            double minx = dataInput.readDouble();
            double miny = dataInput.readDouble();
            double maxx = dataInput.readDouble();
            double maxy = dataInput.readDouble();
            BoundingBox box = new BoundingBox(minx, miny, maxx, maxy);
            job.setBounds(box);
        }

        next = dataInput.readBoolean();
        if(next) {
            int srid = dataInput.readInt();
            SRS srs = SRS.getSRS(srid);
            job.setSrs(srs);
        }
        
        return job;
    }

    @Override
    public void objectToData(JobObject job, TupleOutput output) {
        output.writeString(job.getLayerName());
        output.writeString(job.getState().name());
        output.writeLong(job.getTimeSpent());
        output.writeLong(job.getTimeRemaining());
        output.writeLong(job.getTilesDone());
        output.writeLong(job.getTilesTotal());
        output.writeLong(job.getFailedTileCount());
        output.writeString(job.getGridSetId());
        output.writeInt(job.getThreadCount());
        output.writeInt(job.getZoomStart());
        output.writeInt(job.getZoomStop());
        output.writeString(job.getFormat());
        output.writeString(job.getJobType().name());
        output.writeFloat(job.getThroughput());
        output.writeInt(job.getMaxThroughput());
        output.writeString(job.getPriority().name());
        output.writeString(job.getSchedule());
        output.writeLong(job.getSpawnedBy());
        output.writeBoolean(job.isRunOnce());
        output.writeBoolean(job.isFilterUpdate());
        output.writeString(job.getEncodedParameters());
        output.writeLong(job.getWarnCount());
        output.writeLong(job.getErrorCount());
        
        if(job.getTimeFirstStart() != null) {
            output.writeBoolean(true);
            output.writeLong(job.getTimeFirstStart().getTime());
        } else {
            output.writeBoolean(false);
        }
        if(job.getTimeLatestStart() != null) {
            output.writeBoolean(true);
            output.writeLong(job.getTimeLatestStart().getTime());
        } else {
            output.writeBoolean(false);
        }
        if(job.getTimeFinish() != null) {
            output.writeBoolean(true);
            output.writeLong(job.getTimeFinish().getTime());
        } else {
            output.writeBoolean(false);
        }
        
        BoundingBox box = job.getBounds();
        if(box != null) {
            output.writeBoolean(true);
            output.writeDouble(box.getMinX());
            output.writeDouble(box.getMinY());
            output.writeDouble(box.getMaxX());
            output.writeDouble(box.getMaxY());
        } else {
            output.writeBoolean(false);
        }
        
        SRS srs = job.getSrs();
        if(srs != null) {
            output.writeBoolean(true);
            output.writeInt(srs.getNumber());
        } else {
            output.writeBoolean(false);
        }
    }
    
    @Override
    public void objectToKey(JobObject job, TupleOutput output) {
        keyBinder.objectToEntry(job.getJobId(), output);
    }
}
