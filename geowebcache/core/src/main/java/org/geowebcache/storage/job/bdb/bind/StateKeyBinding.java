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

import org.geowebcache.seed.GWCTask.STATE;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * A TupleBinding implementation for en/decoding the state field of a JobObject.  Used for
 * building secondary keys.
 * 
 * @author mleslie
 */
public class StateKeyBinding extends TupleBinding<STATE> {

    @Override
    public STATE entryToObject(TupleInput input) {
        return STATE.valueOf(input.readString());
    }

    @Override
    public void objectToEntry(STATE object, TupleOutput output) {
        output.writeString(object.name());
    }

}
