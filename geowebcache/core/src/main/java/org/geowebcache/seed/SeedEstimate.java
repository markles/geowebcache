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
package org.geowebcache.seed;

import org.geowebcache.grid.BoundingBox;

/**
 * POJO for building and tracking time and space estimates for seeding tasks, as well
 * as the parameters used to produce the estimates.
 * @author mleslie
 *
 */
public class SeedEstimate {
    public String layerName;
    public String gridSetId;
    public BoundingBox bounds;
    public int zoomStart;
    public int zoomStop;
    public int threadCount;
    public int maxThroughput;
    public String format;

    public long tilesDone;
    public long timeSpent;

    public long tilesTotal;
    public long diskSpace;
    public long timeRemaining;
}
