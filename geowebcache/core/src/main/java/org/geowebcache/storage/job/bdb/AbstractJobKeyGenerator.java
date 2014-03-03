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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.storage.JobObject;
import org.geowebcache.storage.job.bdb.bind.JobObjectEntityBinding;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

/**
 * Provides helpful functionality for generating secondary keys used by the BDBJobStore.
 * 
 * @author mleslie
 */
public abstract class AbstractJobKeyGenerator implements SecondaryKeyCreator {

private static final Log log = LogFactory.getLog(JobStoreIndexer.class);
private JobObjectEntityBinding binding;
public AbstractJobKeyGenerator(JobObjectEntityBinding binding) {
    this.binding = binding;
}

public boolean createSecondaryKey(SecondaryDatabase secondary,
        DatabaseEntry key, DatabaseEntry data, DatabaseEntry result) {

    try {
        JobObject job = binding.entryToObject(key, data);
        return extractKey(job, result);
    } catch(Exception ex) {
        log.error("Secondary key creation failed.  Data will not be indexed.", ex);
        return false;
    }
}

protected abstract boolean extractKey(JobObject job, DatabaseEntry result) throws IOException;


}
