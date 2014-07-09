//$HeadURL$
/*----------------------------------------------------------------------------
 This file is part of deegree, http://deegree.org/
 Copyright (C) 2001-2012 by:
 - Department of Geography, University of Bonn -
 and
 - lat/lon GmbH -

 This library is free software; you can redistribute it and/or modify it under
 the terms of the GNU Lesser General Public License as published by the Free
 Software Foundation; either version 2.1 of the License, or (at your option)
 any later version.
 This library is distributed in the hope that it will be useful, but WITHOUT
 ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 details.
 You should have received a copy of the GNU Lesser General Public License
 along with this library; if not, write to the Free Software Foundation, Inc.,
 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

 Contact information:

 lat/lon GmbH
 Aennchenstr. 19, 53177 Bonn
 Germany
 http://lat-lon.de/

 Department of Geography, University of Bonn
 Prof. Dr. Klaus Greve
 Postfach 1147, 53001 Bonn
 Germany
 http://www.geographie.uni-bonn.de/deegree/

 e-mail: info@deegree.org
 ----------------------------------------------------------------------------*/

package org.deegree.tile.persistence.cassandra;

import java.nio.ByteBuffer;

import org.deegree.tile.Tile;
import org.deegree.tile.TileDataLevel;
import org.deegree.tile.TileMatrix;
import org.deegree.geometry.Envelope;

import static org.deegree.tile.Tiles.calcTileEnvelope;
import org.deegree.tile.persistence.cassandra.db.CassandraConnector;

/**
 * {@link TileDataLevel} implementation for the {@link CassandraTileStore}.
 * 
 * @author <a href="mailto:vieweg@lat-lon.de">Martin Vieweg</a>
 * @author last edited by: $Author$
 * 
 * @version $Revision$, $Date$
 */
public class CassandraTileDataLevel implements TileDataLevel {

    private final TileMatrix metadata;

    private final CassandraConnector caConnector;
    
    /**
     * Creates a new {@link FileSystemTileDataLevel} instance.
     * 
     * @param metadata
     *            TileDataLevel metadata
     * @param caConnector
     * 
     */
    public CassandraTileDataLevel
        (TileMatrix metadata, CassandraConnector caConnector) {
        this.metadata = metadata;
        this.caConnector = caConnector;
    }

    @Override
    public TileMatrix getMetadata() {
        return this.metadata;
    }

    @Override
    public Tile getTile( long x, long y ) {
        if ( metadata.getNumTilesX() <= x || metadata.getNumTilesY() <= y || x < 0 || y < 0 ) {
            return null;
        }
        Envelope bbox = calcTileEnvelope( metadata, x, y );
        ByteBuffer tileImage = caConnector.resolv( metadata.getIdentifier(), x, y);
        return new CassandraTile( bbox, tileImage );
    }


    
}
