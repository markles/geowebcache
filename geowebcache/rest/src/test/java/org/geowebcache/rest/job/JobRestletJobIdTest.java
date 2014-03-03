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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.geowebcache.seed.GWCTask.STATE;
import org.geowebcache.storage.JobObject;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.restlet.data.Method;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;

/**
 * Tests calls to the JobRestlet that do provide a job id, ie. calls to ./job/<jobId>/
 * Note that this tests the JobLogRestlet class, it does not actually stand up the application and 
 * make http requests against it.
 * 
 * @author mleslie
 */
public class JobRestletJobIdTest extends AbstractJobRestletTest {

    @Test
    /**
     * Tests /job/<jobId> endpoint GET method.
     * @throws Exception
     */
    public void testJobIdGet() throws Exception {
        JSONObject jsonJob = getJobById(job2Running.getJobId());
        assertJobEquality(job2Running, jsonJob);
    }
    
    @Test
    /**
     * Tests /job/<jobId> endpoint POST method.
     * @throws Exception
     */
    public void testJobIdPost() throws Exception {
        /*
         * First test is that we cannot kill a job that is not running.
         */
        final Map<String, Object> attrMap = new HashMap<String, Object>();
        attrMap.put("extension", "json");
        attrMap.put("job", job3Done.getJobId());
        Request request = new Request() {
            @Override
            public Map<String, Object> getAttributes() {
                return attrMap;
            }
            @Override
            public Method getMethod() {
                return Method.POST;
            }
        };
        request.setEntity(getJSONRepresentation("job3.json"));

        Response response = new Response(request);
        jobRestlet.handle(request, response);

        assertEquals(Status.SERVER_ERROR_INTERNAL, response.getStatus());
        
        /*
         * Next, we'll check that we can kill a job that is running.
         */
        JobObject job2Killed = JobObject.createJobObject(job2Running);
        job2Killed.setJobId(job2Running.getJobId());
        job2Killed.setState(STATE.KILLED);
        job2Killed.setTimeSpent(job2Running.getTimeSpent());
        job2Killed.setTimeRemaining(job2Running.getTimeRemaining());
        job2Killed.setTilesDone(job2Running.getTilesDone());
        job2Killed.setTilesTotal(job2Running.getTilesTotal());
        job2Killed.setFailedTileCount(job2Running.getFailedTileCount());
        job2Killed.setThroughput(job2Running.getThroughput());
        job2Killed.setTimeFirstStart(job2Running.getTimeFirstStart());
        job2Killed.setTimeLatestStart(job2Running.getTimeLatestStart());

        attrMap.put("job", job2Running.getJobId());
        request = new Request() {
            @Override
            public Map<String, Object> getAttributes() {
                return attrMap;
            }
            @Override
            public Method getMethod() {
                return Method.POST;
            }
        };
        request.setEntity(getJSONRepresentation("job2-killed.json"));

        response = new Response(request);
        jobRestlet.handle(request, response);        

        assertEquals(Status.SUCCESS_OK, response.getStatus());

        JSONObject json = getJobById(job2Running.getJobId());
        assertJobEquality(job2Killed, json);
    }
    
    @Test
    /**
     * Tests the /job/<jobId> endpoint's PUT method.
     * @throws Exception
     */
    public void testJobIdPut() throws Exception {
        /*
         * Test matchings between jobId and job definition.
         */
        final Map<String, Object> attrMap = new HashMap<String, Object>();
        attrMap.put("extension", "json");
        attrMap.put("job", 4l);
        Request request = new Request() {
            @Override
            public Map<String, Object> getAttributes() {
                return attrMap;
            }
            @Override
            public Method getMethod() {
                return Method.PUT;
            }
        };
        request.setEntity(getJSONRepresentation("job4.json"));

        Response response = new Response(request);
        jobRestlet.handle(request, response);
        
        assertEquals(Status.SUCCESS_OK, response.getStatus());
        JSONObject jsonJob = getJobById(4l);
        assertNotNull(jsonJob);
        
        /*
         * Test mismatch between jobId and job definition.
         */
        attrMap.put("job", 7l);
        request = new Request() {
            @Override
            public Map<String, Object> getAttributes() {
                return attrMap;
            }
            @Override
            public Method getMethod() {
                return Method.PUT;
            }
        };
        request.setEntity(getJSONRepresentation("job8.json"));

        response = new Response(request);
        jobRestlet.handle(request, response);
        
        assertEquals(Status.SUCCESS_OK, response.getStatus());
        // The job id is ignored in favor of using the internal system to avoid collisions.
        jsonJob = getJobById(5l);
        assertNotNull(jsonJob);
        JobObject job8 = getJob("job8.json");
        job8.setJobId(5l);
        // Job8 isn't scheduled, so we need to ensure that the timeFirstStart is set, and then remove it from other comparisons
        assertTrue("New job has been started", jsonJob.has("timeFirstStart"));
        jsonJob.remove("timeFirstStart");
        assertJobEquality(job8, jsonJob);
        }
    
    @Test 
    /**
     * Tests the /job/<jobId> endpoint's DELETE method.
     * @throws Exception
     */
    public void testJobIdDelete() throws Exception {
        final Map<String, Object> attrMap = new HashMap<String, Object>();
        attrMap.put("job", Long.valueOf(job1Ready.getJobId()).toString());
        attrMap.put("extension", "json");
        Request request = new Request() {
            @Override
            public Map<String, Object> getAttributes() {
                return attrMap;
            }
            @Override
            public Method getMethod() {
                return Method.DELETE;
            }
        };
        Response response = new Response(request);

        jobRestlet.handle(request, response);
        assertEquals(Status.SUCCESS_OK, response.getStatus());
        assertNull(response.getEntity());
        
        attrMap.put("job", Long.valueOf(job3Done.getJobId()).toString());
        response = new Response(request);
        
        jobRestlet.handle(request, response);
        assertEquals(Status.SUCCESS_OK, response.getStatus());
        assertNull(response.getEntity());
        
        attrMap.put("job", Long.valueOf(job2Running.getJobId()).toString());
        response = new Response(request);
        
        jobRestlet.handle(request, response);
        assertEquals(Status.SUCCESS_OK, response.getStatus());
        assertNull(response.getEntity());
    }
    
    private JSONObject getJobById(Long jobId) throws Exception {
        Representation rep = getRepresentationForJobId(jobId);
        String result = rep.getText();
        JSONObject root = new JSONObject(result);
        assertEquals(1, root.length());
        JSONArray array = root.getJSONArray("singleton-set");
        assertEquals(1, array.length());
        JSONObject json = array.getJSONObject(0);
        assertNotNull(json);
        return json;
    }
    
    private Representation getRepresentationForJobId(Long jobId) throws Exception {
        final Map<String, Object> attrMap = new HashMap<String, Object>();
        attrMap.put("job", jobId.toString());
        attrMap.put("extension", "json");
        Request request = new Request() {
            @Override
            public Map<String, Object> getAttributes() {
                return attrMap;
            }
            @Override
            public Method getMethod() {
                return Method.GET;
            }
        };
        Response response = new Response(request);

        jobRestlet.handle(request, response);
        Representation rep = response.getEntity();
        assertNotNull(rep);
        return rep;
    }
    
}
