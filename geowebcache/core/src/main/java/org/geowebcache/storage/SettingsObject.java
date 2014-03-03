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
package org.geowebcache.storage;

/**
 * Responsible for the configuration details of the job management subsystem.
 */
public class SettingsObject {

    private long clearOldJobs = -1;

    public long getClearOldJobs() {
        return clearOldJobs;
    }

    public void setClearOldJobs(long clearOldJobs) {
        this.clearOldJobs = clearOldJobs;
    }

}
