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
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
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
        
    private final boolean tileTimestamp = true;
    
    // static variables
    private final static char separatorChar = '|';

    /**
     * Creates a new {@link CassandraDB} instance.
     * 
     * @param hosts
     *          cassandra database hostnames (and port) to connect, must not be <code>null</code>
     * @param keyspace
     *          used keyspace, must not be <code>null</code>
     */
    public CassandraDB( String hosts, String keyspace ) {
        this.hosts = hosts;
        this.keyspaceName = keyspace;

        this.connect();
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
     * @param columnFamily
     * 
     * @return
     */
    public Row getRow( String key, String columnFamily ) {
        Row res = null;

        Statement getTileStatement = new SimpleStatement(
                "SELECT * FROM " + columnFamily
                + " WHERE key = \'" + key + "\'" )
                .setConsistencyLevel(ConsistencyLevel.ONE);        
        try {
            res = session.execute(getTileStatement).one();
        } catch ( Exception e ) {
            throw new TileIOException( "Error while querying cassandra db, " + e.getMessage() );
        }
        if ( res != null && tileTimestamp == true )
            setTileTimestamp( key, columnFamily );
        
        return res;
    }
    
    private void setTileTimestamp(String key, String columnFamily) {
        Statement updateTileTimestamp = new SimpleStatement(
                "UPDATE " + columnFamily
                + " SET tileTimestamp = " + System.currentTimeMillis()
                + "WHERE key = \'" + key + "\'")
                .setConsistencyLevel(ConsistencyLevel.ONE);

        try {
            session.execute(updateTileTimestamp);
        } catch (Exception e) {
            System.err.println(
                    "Problem writing lru value for key " + key
                    + ", Error: " + e);
        }
    }
    
    public char getSeparatorChar() {
        return this.separatorChar;
    }

}
