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

package org.deegree.tile.persistence.cassandra.db;

import com.datastax.driver.core.Row;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import org.deegree.tile.TileDataLevel;
import org.deegree.tile.TileDataSet;

/**
 * <p>
 * Create the connection between Cassandra Database communication class,
 * TileDataSet and associated TileDataLevels.
 * </p>
 * 
 * @author <a href="mailto:vieweg@lat-lon.de">Martin Vieweg</a>
 * 
 */
public class CassandraConnector {
    
    private TileDataSet tds;
    
    final private CassandraDB cassandraDB;
    
    final private String columnFamily;
    
    /**
     *
     * @param cassandraDB
     * @param columnFamily
     */
    public CassandraConnector(CassandraDB cassandraDB, String columnFamily) {
        this.cassandraDB = cassandraDB;
        this.columnFamily = columnFamily;
    }
    
    /**
     *
     * @return
     */
    public String getColumnFamily() {
        return this.columnFamily;
    }
    
    /**
     * Set corresponfing TileDataSet
     * @param tds
     */
    public void setTileDataSet(TileDataSet tds) {
        this.tds = tds;
    }
    
    /**
     * get corresponfing TileDataSet
     * @return TileDataSet
     */
    public TileDataSet getTileDataSet() {
        return this.tds;
    }

    /**
     * Returns the image file for the specified {@link org.deegree.tile.TileDataLevel} and tile indexes.
     * 
     * @param matrixId
     *            identifier of the matrix in the matrix set, must not be <code>null</code>
     * @param x
     *            column index of the tile (starting at 0)
     * @param y
     *            row index of the tile (starting at 0)
     * @return tile file or <code>null</code> if the tile matrix does not exist (or indexes are out of range)
     */
    public ByteBuffer resolv( String matrixId, long x, long y ) {
        TileDataLevel tileMatrix = tds.getTileDataLevel( matrixId );
        if ( tileMatrix == null ) {
            return null;
        }
        if ( tileMatrix.getMetadata().getNumTilesX() <= x || tileMatrix.getMetadata().getNumTilesY() <= y || x < 0
             || y < 0 ) {
            return null;
        }
        
        StringBuilder rowKey = new StringBuilder();
        String levelDirectory = getLevelDirectory( tileMatrix );
        String columnFileNamePart = getColumnFileNamePart( x );
        String rowFileNamePart = getRowFileNamePart( y, tileMatrix );

        rowKey.append( tds.getNativeImageFormat().replaceAll( "image/", "" ) );
        rowKey.append( cassandraDB.getSeparatorChar() );
        rowKey.append( levelDirectory );
        rowKey.append( cassandraDB.getSeparatorChar() );
        rowKey.append( columnFileNamePart );
        rowKey.append( cassandraDB.getSeparatorChar() );        
        rowKey.append( rowFileNamePart );

        Row row = cassandraDB.getRow( rowKey.toString(), this.columnFamily );
        if ( row == null ) {
            return null;
        }
        
        return row.getBytes( "img" );
    }
    
     private String getLevelDirectory( TileDataLevel tileMatrix ) {
        DecimalFormat formatter = new DecimalFormat( "00" );
        int tileMatrixIndex = tds.getTileDataLevels().indexOf( tileMatrix );
        return formatter.format( tileMatrixIndex );
    }

    private String getColumnFileNamePart( long x ) {
        StringBuilder sb = new StringBuilder();
        DecimalFormat formatter = new DecimalFormat( "000" );
        sb.append( formatter.format( x / 1000000 ) );
        sb.append( cassandraDB.getSeparatorChar() );
        sb.append( formatter.format( x / 1000 % 1000 ) );
        sb.append( cassandraDB.getSeparatorChar() );
        sb.append( formatter.format( x % 1000 ) );
        return sb.toString();
    }

    private String getRowFileNamePart( long y, TileDataLevel tileMatrix ) {
        long tileCacheY = getTileCacheYIndex( tileMatrix, y );
        StringBuilder sb = new StringBuilder();
        DecimalFormat formatter = new DecimalFormat( "000" );
        sb.append( formatter.format( tileCacheY / 1000000 ) );
        sb.append( cassandraDB.getSeparatorChar() );
        sb.append( formatter.format( tileCacheY / 1000 % 1000 ) );
        sb.append( cassandraDB.getSeparatorChar() );
        sb.append( formatter.format( tileCacheY % 1000 ) );
        return sb.toString();
    }    

    private long getTileCacheYIndex( TileDataLevel tileMatrix, long y ) {
        // TileCache's y-axis is inverted
        return tileMatrix.getMetadata().getNumTilesY() - 1 - y;
    }
    
}
