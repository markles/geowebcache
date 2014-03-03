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
 * 
 * @author Marius Suta / The Open Planning Project 2008
 * @author Arne Kepp / The Open Planning Project 2009  
 */
package org.geowebcache.rest.seed;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.config.ContextualConfigurationProvider;
import org.geowebcache.config.XMLConfiguration;
import org.geowebcache.rest.GWCRestlet;
import org.geowebcache.rest.RestletException;
import org.json.JSONObject;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

/**
 * Shared code for MassTruncateRestlet and SeedRestlet
 */
abstract public class GWCSeedingRestlet extends GWCRestlet {
    @SuppressWarnings("unused")
    private static Log log = LogFactory.getLog(GWCSeedingRestlet.class);

    public JSONObject myrequest;

    protected XMLConfiguration xmlConfig;

    public void handle(Request request, Response response) {
        Method met = request.getMethod();
        try {
            if (met.equals(Method.GET)) {
                doGet(request, response);
            } else if (met.equals(Method.POST)) {
                doPost(request, response);
            } else {
                throw new RestletException("Method not allowed",
                        Status.CLIENT_ERROR_METHOD_NOT_ALLOWED);
            }
        } catch (RestletException re) {
            response.setEntity(re.getRepresentation());
            response.setStatus(re.getStatus());
        } catch (IOException ioe) {
            response.setEntity("Encountered IO error " + ioe.getMessage(), MediaType.TEXT_PLAIN);
            response.setStatus(Status.SERVER_ERROR_INTERNAL);
        }
    }

    /**
     * Handle a GET request
     */
    abstract public void doGet(Request req, Response resp) throws RestletException;

    /**
     * Handle a POST request
     */
    public void doPost(Request req, Response resp) throws RestletException, IOException {
        String formatExtension = (String) req.getAttributes().get("extension");

        XStream xs = configXStream(new XStream(new DomDriver()));

        Object obj = null;
        
        if (formatExtension==null || formatExtension.equalsIgnoreCase("xml")) {
            obj = xs.fromXML(req.getEntity().getStream());
        } else if (formatExtension.equalsIgnoreCase("json")) {
            obj = xs.fromXML(convertJson(req.getEntity().getText()));
        } else {
            throw new RestletException("Format extension unknown or not specified: "
                    + formatExtension, Status.CLIENT_ERROR_BAD_REQUEST);
        }

        handleRequest(req, resp, obj);

    }

    abstract protected void handleRequest(Request req, Response resp, Object obj);
    
    public void setXmlConfig(XMLConfiguration xmlConfig) {
        this.xmlConfig = xmlConfig;
    }
    
    protected XStream configXStream(XStream xs) {
        return xmlConfig.getConfiguredXStreamWithContext(xs, ContextualConfigurationProvider.Context.REST);
    }
}
