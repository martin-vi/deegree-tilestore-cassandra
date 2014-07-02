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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.deegree.tile.TileDataLevel;
import org.deegree.tile.TileDataSet;
import org.deegree.tile.TileIOException;

/**
 * Cassandra Database connection and schema implementation, tile key is inspired
 * by <a href="http://tilecache.org/">TileCache</a>, see also deegree 
 * {@link DiskLayout} implementation.<br/>
 * <br/>
 * tilecache {
 *  key: png|zz|xxx|xxx|xxx|yyy|yyy|yyy { img: [BLOB], lru [long], ... }
 *  ...
 * }
 * 
 * @author <a href="mailto:vieweg@lat-lon.de">Martin Vieweg</a>
 * @author last edited by: $Author$
 * 
 * @version $Revision$, $Date$
 */
public class CassandraDB {

    // cassandra instance variables
    private Cluster cluster;
    private Session session;    
    
    // configure variables
    private final String hosts;
    private final String keyspaceName;
    private final String columnFamily;
    private String fileType = "png";
    private boolean LRUenabled = true;
    
    // wmts settings
    private TileDataSet set;
    
    // static variables
    private final static String separatorChar = "|";

    /**
     * Creates a new {@link CassandraDB} instance.
     * 
     * @param hosts
     *          cassandra database hostnames (and port) to connect, must not be <code>null</code>
     * @param keyspace
     *          used keyspace, must not be <code>null</code>
     * @param columnFamily
     *          used columnFamily, must not be <code>null</code>
     */
    public CassandraDB( String hosts, String keyspace, String columnFamily ) {
        this.hosts = hosts;
        this.keyspaceName = keyspace;
        this.columnFamily = columnFamily;

        this.connect();
    }
    
    /**
     * Assigns the given {@link TileDataSet}.
     * 
     * @param set
     *            tile matrix to assign, must not be <code>null</code>
     */
    public void setTileMatrixSet( TileDataSet set ) {
        this.set = set;
    }    
    
    /**
     * Connects to Cassandra Database.
     */
    private void connect() {
        
        Collection<InetAddress> hostCollection = new ArrayList<InetAddress>();
        for ( String h : hosts.split(",") ) {
            try {
                hostCollection.add(InetAddress.getByName( h ) );
            } catch (UnknownHostException ex) {
                Logger.getLogger(CassandraDB.class.getName()).log(Level.SEVERE, null, ex);
                continue;
            }
        }
        
        cluster = Cluster.builder()
                .addContactPoints(hostCollection)
                .withLoadBalancingPolicy(new LatencyAwarePolicy.Builder(new RoundRobinPolicy()).build())
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))                
                .build();
        
        // ToDO
        // Check ... Metadata metadata = cluster.getMetadata();
        // Test for ColumnFamily!

        session = cluster.connect(keyspaceName);
        if (session == null) {
            throw new TileIOException( "Keyspace " + keyspaceName + " not exsisting." );
        }        
    }
    
    /**
     * Fetch single Cassandra row.
     * 
     * @param key
     *          Key to identify and access a Cassandra row.
     * @return
     */
    private Row getRow( String key ) {
        Row res = null;

        // ToDo configurable ConsistencyLevel
        Statement getTileStatement = new SimpleStatement(
                "SELECT * FROM " + columnFamily
                + " WHERE key = \'" + key + "\'" )
                .setConsistencyLevel(ConsistencyLevel.ONE);        
        try {
            res = session.execute(getTileStatement).one();
        } catch ( Exception e ) {
            throw new TileIOException( "Error while querying cassandra db, " + e.getMessage() );
        }
        
        return res;
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
        TileDataLevel tileMatrix = set.getTileDataLevel( matrixId );
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

        rowKey.append( fileType );
        rowKey.append( separatorChar );
        rowKey.append( levelDirectory );
        rowKey.append( separatorChar );
        rowKey.append( columnFileNamePart );
        rowKey.append( separatorChar );        
        rowKey.append( rowFileNamePart );

        Row row = this.getRow( rowKey.toString() );
        if ( row == null ) {
            return null;
        }

        if ( LRUenabled ) {
            Statement updateLRUStatement = new SimpleStatement(
                    "UPDATE " + columnFamily 
                            +" SET lru = " + System.currentTimeMillis()
                            + "WHERE key = \'" + rowKey.toString() + "\'" )
                .setConsistencyLevel(ConsistencyLevel.ONE);

            try {
                session.execute(updateLRUStatement);
            } catch ( Exception e ) {
                System.err.println("Problem writing lru value for key " + rowKey.toString()
                    + ", Error: " + e );
            }
        }
        
        return row.getBytes( "img" );
    }
    
     private String getLevelDirectory( TileDataLevel tileMatrix ) {
        DecimalFormat formatter = new DecimalFormat( "00" );
        int tileMatrixIndex = set.getTileDataLevels().indexOf( tileMatrix );
        return formatter.format( tileMatrixIndex );
    }

    private String getColumnFileNamePart( long x ) {
        StringBuilder sb = new StringBuilder();
        DecimalFormat formatter = new DecimalFormat( "000" );
        sb.append( formatter.format( x / 1000000 ) );
        sb.append( separatorChar );
        sb.append( formatter.format( x / 1000 % 1000 ) );
        sb.append( separatorChar );
        sb.append( formatter.format( x % 1000 ) );
        return sb.toString();
    }

    private String getRowFileNamePart( long y, TileDataLevel tileMatrix ) {
        long tileCacheY = getTileCacheYIndex( tileMatrix, y );
        StringBuilder sb = new StringBuilder();
        DecimalFormat formatter = new DecimalFormat( "000" );
        sb.append( formatter.format( tileCacheY / 1000000 ) );
        sb.append( separatorChar );
        sb.append( formatter.format( tileCacheY / 1000 % 1000 ) );
        sb.append( separatorChar );
        sb.append( formatter.format( tileCacheY % 1000 ) );
        return sb.toString();
    }
    
    private long getTileCacheYIndex( TileDataLevel tileMatrix, long y ) {
        // TileCache's y-axis is inverted
        return tileMatrix.getMetadata().getNumTilesY() - 1 - y;
    }
    
    // ToDo JavaDoc
    public void setFileType( String fileType ) {
        this.fileType = fileType;
    }
    
    public String getFileType() {
        return fileType;
    }
    
    public void enableLRU() {
        this.LRUenabled = true;
    }
    
    public void disableLRU() {
        this.LRUenabled = false;
    }

}
