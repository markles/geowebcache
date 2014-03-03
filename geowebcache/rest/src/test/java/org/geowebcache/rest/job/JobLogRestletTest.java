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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.restlet.data.Method;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;

/**
 * Tests the various rest calls against the JobLogRestlet.  Note that this tests the JobLogRestlet
 * class, it does not actually stand up the application and make http requests against it.
 * 
 * @author mleslie
 */
public class JobLogRestletTest extends AbstractJobRestletTest {
    
    @Test
    /**
     * Tests the /job/<jobId>/logs endpoint's GET method
     */
    public void testJobLogGet() throws Exception {
        /*
         * Verify the success case.
         */
        final Map<String, Object> attrMap = new HashMap<String, Object>();
        attrMap.put("job", Long.valueOf(job2Running.getJobId()).toString());
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
    
        jobLogRestlet.handle(request, response);
        assertEquals(Status.SUCCESS_OK, response.getStatus());
        
        Representation rep = response.getEntity();
        assertNotNull(rep);
        String result = rep.getText();
        JSONObject root = new JSONObject(result);
        assertTrue("Expecting logs.", root.has("logs"));
        JSONArray array = root.getJSONArray("logs");
        assertEquals("Expecting 6 logs.", 6l, array.length());
        
        /*
         * Our first failure case is to not set the job id.
         */
        attrMap.remove("job");
        request = new Request() {
            @Override
            public Map<String, Object> getAttributes() {
                return attrMap;
            }
            @Override
            public Method getMethod() {
                return Method.GET;
            }
        };
        response = new Response(request);
    
        jobLogRestlet.handle(request, response);
        assertEquals(Status.CLIENT_ERROR_BAD_REQUEST, response.getStatus());
        
        /*
         * Next we check an invalid job id.
         */
        attrMap.put("job", "18");
        request = new Request() {
            @Override
            public Map<String, Object> getAttributes() {
                return attrMap;
            }
            @Override
            public Method getMethod() {
                return Method.GET;
            }
        };
        response = new Response(request);
    
        jobLogRestlet.handle(request, response);
        assertEquals(Status.CLIENT_ERROR_BAD_REQUEST, response.getStatus());
        
    }
    
    @Test
    /**
     * Tests the /job/<jobId>/logs endpoint's POST method
     */
    public void testJobLogPost() throws Exception {
        final Map<String, Object> attrMap = new HashMap<String, Object>();
        attrMap.put("job", Long.valueOf(job2Running.getJobId()).toString());
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
        request.setEntity(getJSONRepresentation("log04.json"));
        Response response = new Response(request);
        
        jobLogRestlet.handle(request, response);
        assertEquals(Status.CLIENT_ERROR_METHOD_NOT_ALLOWED, response.getStatus());
    }
    
    @Test
    /**
     * Tests the /job/<jobId>/logs endpoint's PUT method
     */
    public void testJobLogPut() throws Exception {
        final Map<String, Object> attrMap = new HashMap<String, Object>();
        attrMap.put("job", Long.valueOf(job2Running.getJobId()).toString());
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
        request.setEntity(getJSONRepresentation("log18.json"));
        Response response = new Response(request);
        
        jobLogRestlet.handle(request, response);
        assertEquals(Status.CLIENT_ERROR_METHOD_NOT_ALLOWED, response.getStatus());
    }
    
    @Test
    /**
     * Tests the /job/<jobId>/logs endpoint's DELETE method
     */
    public void testJobLogDelete() throws Exception {
        final Map<String, Object> attrMap = new HashMap<String, Object>();
        attrMap.put("job", Long.valueOf(job2Running.getJobId()).toString());
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
        Response response = new Response(request);
        
        jobLogRestlet.handle(request, response);
        assertEquals(Status.CLIENT_ERROR_METHOD_NOT_ALLOWED, response.getStatus());
    }
}

