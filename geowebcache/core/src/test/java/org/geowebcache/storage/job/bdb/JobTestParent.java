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

import static org.easymock.EasyMock.expect;
import static org.easymock.classextension.EasyMock.replay;
import static org.junit.Assert.assertNotNull;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Date;

import org.easymock.classextension.EasyMock;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.grid.BoundingBox;
import org.geowebcache.grid.GridSet;
import org.geowebcache.grid.GridSetFactory;
import org.geowebcache.grid.GridSubset;
import org.geowebcache.grid.SRS;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.seed.GWCTask.PRIORITY;
import org.geowebcache.seed.GWCTask.STATE;
import org.geowebcache.seed.GWCTask.TYPE;
import org.geowebcache.seed.SeedRequest;
import org.geowebcache.storage.JobLogObject;
import org.geowebcache.storage.JobObject;

/**
 * Provides a handful of convenience methods to build JobObjects and JobLogObjects.
 * 
 * @author mleslie
 */
public class JobTestParent {

  protected JobObject buildDefaultJob() throws GeoWebCacheException {
      return generateDefaultJob();
  }
  
  protected JobObject buildJob(long id) throws GeoWebCacheException {
      JobObject job = generateDefaultJob();
      job.setJobId(id);
      assertNotNull(job);
      return job;
  }
  
  protected JobObject buildJob(long id, STATE state) throws GeoWebCacheException {
      JobObject job = buildJob(id);
      job.setState(state);
      assertNotNull(job);
      return job;
  }
  
  protected JobObject buildJob(long id, STATE state, Timestamp timeFinish) 
          throws GeoWebCacheException {
      JobObject job = buildJob(id, state);
      job.setTimeFinish(timeFinish);
      assertNotNull(job);
      return job;
  }
  
  private GridSubset sensibleGridSubset() {
      GridSet gs = GridSetFactory.createGridSet("EPSG:4326", SRS.getEPSG4326(), BoundingBox.WORLD4326,
              true, 20, 111320.0, 1.0, 256, 256, false);
      return new GridSubset(gs, null, BoundingBox.WORLD4326, true, 0, 10);
  }
  
  private JobObject generateDefaultJob() throws GeoWebCacheException {
      TileLayer tl = EasyMock.createMock(TileLayer.class);
      expect(tl.getGridSubset("EPSG:4326")).andReturn(sensibleGridSubset());
      expect(tl.getGridSubsets()).andReturn(Collections.singleton("EPSG:4326"));
      replay(tl);
      SeedRequest sr = new SeedRequest("topp:tasmania", BoundingBox.WORLD4326, "EPSG:4326", 
              2, 0, 11, "image/png8", TYPE.SEED, PRIORITY.NORMAL, null, true, 10, null);
      JobObject job = JobObject.createJobObject(tl, sr);
      return job;
  }
  
  protected JobLogObject createDefaultInfoLog(long jobId) throws GeoWebCacheException {
      return createDefaultInfoLog(buildJob(jobId));
  }
  
  protected JobLogObject createDefaultInfoLog(JobObject job) {
      JobLogObject log = JobLogObject.createWarnLog(job.getJobId(), "Info Log", "Nothing to worry about at " + new Date().toString());
      job.addLog(log);
      return log;
  }
  
  protected JobLogObject createDefaultWarnLog(long jobId) throws GeoWebCacheException {
      return createDefaultWarnLog(buildJob(jobId));
  }

  protected JobLogObject createDefaultWarnLog(JobObject job) {
      JobLogObject log = JobLogObject.createWarnLog(job.getJobId(), "Warning Log", "Log warned at " + new Date().toString());
      job.addLog(log);
      return log;
  }
  
  protected JobLogObject createDefaultErrorLog(long jobId) throws GeoWebCacheException {
      return createDefaultErrorLog(buildJob(jobId));
  }

  protected JobLogObject createDefaultErrorLog(JobObject job) {
      JobLogObject log = JobLogObject.createWarnLog(job.getJobId(), "Error Log", "Massive badness at " + new Date().toString());
      job.addLog(log);
      return log;
  }

}