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

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * A TupleBinding implementation for en/decoding the jobLogId of a JobLogObject.
 * 
 * @author mleslie
 */
public class JobLogObjectKeyBinding extends TupleBinding<Long> {

@Override
public Long entryToObject(TupleInput input) {
    return input.readPackedLong();
}

@Override
public void objectToEntry(Long object, TupleOutput output) {
    output.writePackedLong(object);
}

}
