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

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import org.deegree.tile.TileDataLevel;
import org.deegree.tile.TileDataSet;
import org.deegree.tile.TileIOException;

/**
 * Cassandra Database connection and schema implementation, tile key is inspired
 * by <a href="http://tilecache.org/">TileCache</a>, see also deegree 
 * {@link DiskLayout} implementation.<br/>
 * <br/>
 * tilecache {
 *  key: zz|xxx|xxx|xxx|yyy|yyy|yyy { filetype: png, img: [BYTE DATA], ... }
 *  ...
 * }
 * 
 * @author <a href="mailto:vieweg@lat-lon.de">Martin Vieweg</a>
 * @author last edited by: $Author$
 * 
 * @version $Revision$, $Date$
 */
public class CassandraDB {

    private Cluster myCluster;
    private KeyspaceDefinition keyspaceDef;
    private Keyspace ksp;
    private ColumnFamilyDefinition colfam = null;
    private ColumnFamilyTemplate<String, String> template;
    
    private TileDataSet set;
    
    private final static String separatorChar = "|";
    
    private final String hosts;
    private final String keyspaceName;
    private final String columnName;
    private final String clusterName;

    /**
     * Creates a new {@link CassandraDB} instance.
     * 
     * @param host
     *          cassandra database hostname (and port) to connect, must not be <code>null</code>
     * @param cluster
     *          define used cluster name
     * @param keyspace
     *          used keyspace, must not be <code>null</code>
     * @param columnFamily
     *          used columnFamily, must not be <code>null</code>
     */
    public CassandraDB( String host, String cluster, String keyspace, String columnFamily ) {
        this.hosts = host;
        this.clusterName = cluster;
        this.keyspaceName = keyspace;
        this.columnName = columnFamily;
        
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
        CassandraHostConfigurator cassandraHostConfigurator = 
                new CassandraHostConfigurator(this.hosts);
        
        cassandraHostConfigurator.setAutoDiscoverHosts(true);
        cassandraHostConfigurator.setAutoDiscoveryDelayInSeconds(30);

        myCluster = HFactory.getOrCreateCluster( clusterName, cassandraHostConfigurator );
        
        keyspaceDef = myCluster.describeKeyspace( keyspaceName );
        if (keyspaceDef == null) {
            throw new TileIOException( "Keyspace " + keyspaceName + " not exsisting." );
        } else {
            ksp = HFactory.createKeyspace( keyspaceName, myCluster );
        }
        
        for ( ColumnFamilyDefinition cfDef : keyspaceDef.getCfDefs() ) {
            if ( ! cfDef.getName().equals( columnName ) ) continue;
            colfam = cfDef;
            break;
        }
        if ( colfam == null ) {
            throw new TileIOException( "ColumnFamily " + columnName + " not exsisting." );
        }

        template = new ThriftColumnFamilyTemplate<String, String>(
                ksp,
                colfam.getName(),
                StringSerializer.get(),
                StringSerializer.get()
        );
    }
    
    /**
     * Fetch single Cassandra row.
     * 
     * @param key
     *          Key to identify and access a Cassandra row.
     * @return
     */
    private ColumnFamilyResult<String, String> getRow( String key ) {
        ColumnFamilyResult<String, String> res = null;
        
        try {
            res = template.queryColumns( key );            
        } catch ( HectorException e ) {
            throw new TileIOException( "Error while querying cassandra db, " + e.getMessage() );
        }
        
        return res;
    }

    public void setConsistencyLevels(HConsistencyLevel readCfConsistencyLvl, HConsistencyLevel writeCfConsistencyLvl) {
        ConfigurableConsistencyLevel configurableConsistencyLevel = new ConfigurableConsistencyLevel();
        Map<String, HConsistencyLevel> rclmap = new HashMap<String, HConsistencyLevel>();
        Map<String, HConsistencyLevel> wclmap = new HashMap<String, HConsistencyLevel>();
        rclmap.put(columnName, readCfConsistencyLvl);
        wclmap.put(columnName, writeCfConsistencyLvl);        

        configurableConsistencyLevel.setReadCfConsistencyLevels(rclmap);
        configurableConsistencyLevel.setWriteCfConsistencyLevels(wclmap);
        ksp.setConsistencyLevelPolicy(configurableConsistencyLevel);
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
    public byte[] resolv( String matrixId, long x, long y ) {
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

        rowKey.append( levelDirectory );
        rowKey.append( separatorChar );
        rowKey.append( columnFileNamePart );
        rowKey.append( rowFileNamePart );

        ColumnFamilyResult<String, String> row = this.getRow( rowKey.toString() );
        if ( row == null ) {
            return null;
        }
        
        return row.getByteArray( "img" );
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
        sb.append( separatorChar );
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
    
    protected void finalize() throws Throwable
    {
      //do finalization here
      super.finalize(); //not necessary if extending Object.
      
    }
   
}
