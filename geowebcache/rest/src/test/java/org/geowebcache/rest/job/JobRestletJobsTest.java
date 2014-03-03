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
 * Tests calls to the JobRestlet that do not provide a job id, ie. calls to ./jobs/
 * Note that this tests the JobLogRestlet class, it does not actually stand up the application and 
 * make http requests against it.
 * 
 * @author mleslie
 */
public class JobRestletJobsTest extends AbstractJobRestletTest {

    @Test
    /**
     * Tests /jobs endpoint GET method.
     * @throws Exception
     */
    public void testJobsGet() throws Exception {
        JSONArray array = requestAllJobs();
        assertNotNull(array);
        assertEquals(3, array.length());

        JSONObject json = array.getJSONObject(0);
        assertJobEquality(job1Ready, json);
        json = array.getJSONObject(1);
        assertJobEquality(job2Running, json);
        json = array.getJSONObject(2);
        assertJobEquality(job3Done, json);
    }

    @Test
    /**
     * Tests the /jobs endpoints POST method.
     */
    public void testJobsPost() throws Exception {
        /*
         * First test is that we cannot kill a job that is not running.
         */
        final Map<String, Object> attrMap = new HashMap<String, Object>();
        attrMap.put("extension", "json");
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

        /**
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

        /*
         * First test is that we cannot kill a job that is not running.
         */
        request.setEntity(getJSONRepresentation("job2-killed.json"));

        response = new Response(request);
        jobRestlet.handle(request, response);        

        assertEquals(Status.SUCCESS_OK, response.getStatus());

        JSONArray array = requestAllJobs();
        assertEquals(3, array.length());

        JSONObject json = array.getJSONObject(0);
        assertJobEquality(job1Ready, json);
        json = array.getJSONObject(1);
        assertJobEquality(job2Killed, json);
        json = array.getJSONObject(2);
        assertJobEquality(job3Done, json);
    }

    @Test
    /**
     * Tests the /jobs endpoint's PUT method.
     * @throws Exception
     */
    public void testJobsPut() throws Exception {
        final Map<String, Object> attrMap = new HashMap<String, Object>();
        attrMap.put("extension", "json");
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

        JSONArray array = requestAllJobs();
        assertNotNull(array);
        assertEquals(4, array.length());

        JSONObject json = array.getJSONObject(0);
        assertJobEquality(job1Ready, json);
        json = array.getJSONObject(1);
        assertJobEquality(job2Running, json);
        json = array.getJSONObject(2);
        assertJobEquality(job3Done, json);
        json = array.getJSONObject(3);
        assertEquals(4l, json.getLong("jobId"));
        assertEquals(STATE.UNSET.name(), json.getString("state"));
    }

    @Test
    /**
     * Tests the /jobs endpoints DELETE method.
     */
    public void testJobsDelete() throws Exception {
        final Map<String, Object> attrMap = new HashMap<String, Object>();
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
        request.setEntity(getJSONRepresentation("job3.json"));

        Response response = new Response(request);
        jobRestlet.handle(request, response);
        assertEquals(Status.CLIENT_ERROR_BAD_REQUEST, response.getStatus());
    }

    protected JSONArray requestAllJobs() throws Exception {
        final Map<String, Object> attrMap = new HashMap<String, Object>();
        attrMap.put("extension", "json");
        attrMap.put("limit", 0);
        attrMap.put("page", 1);
        attrMap.put("start", 0);
        attrMap.put("sort", "[{\"property\":\"jobId\",\"direction\":\"ASC\"}]");
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
        assertEquals(Status.SUCCESS_OK, response.getStatus());
        Representation rep = response.getEntity();
        assertNotNull(rep);
        String result = rep.getText();
        JSONObject root = new JSONObject(result);
        assertEquals(1, root.length());
        JSONArray array = root.getJSONArray("jobs");
        return array;
    }
}
