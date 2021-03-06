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

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.deegree.commons.config.DeegreeWorkspace;
import org.deegree.commons.config.ResourceInitException;
import org.deegree.commons.config.ResourceManager;
import static org.deegree.commons.xml.jaxb.JAXBUtils.unmarshall;
import org.deegree.tile.DefaultTileDataSet;
import org.deegree.tile.TileDataLevel;
import org.deegree.tile.TileDataSet;
import org.deegree.tile.TileMatrix;
import org.deegree.tile.TileMatrixSet;
import org.deegree.tile.persistence.TileStoreProvider;
import org.deegree.tile.persistence.cassandra.db.CassandraConnector;
import org.deegree.tile.persistence.cassandra.db.CassandraDB;
import org.deegree.tile.persistence.cassandra.jaxb.CassandraTileStoreJAXB;
import org.deegree.tile.tilematrixset.TileMatrixSetManager;

import org.slf4j.Logger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * {@link TileStoreProvider} for the {@link CassandraTileStore}.
 * 
 * @author <a href="mailto:vieweg@lat-lon.de">Martin Vieweg</a>
 * @author last edited by: $Author$
 * 
 * @version $Revision$, $Date$
 */
public class CassandraTileStoreProvider implements TileStoreProvider {
    
    private static final Logger LOG = getLogger( CassandraTileStoreProvider.class );

    private static final String CONFIG_NAMESPACE = "http://www.deegree.org/datasource/tile/cassandra";

    private static final URL CONFIG_SCHEMA = CassandraTileStoreProvider.class.getResource( "/META-INF/schemas/datasource/tile/cassandra/3.2.0/cassandra.xsd" );

    private static final String JAXB_PACKAGE = "org.deegree.tile.persistence.cassandra.jaxb";
    
    private DeegreeWorkspace workspace;

    @Override
    public void init( DeegreeWorkspace workspace ) {
        this.workspace = workspace;
    }

    @Override
    public CassandraTileStore create( URL configUrl ) throws ResourceInitException {
        try {

            CassandraTileStoreJAXB config = (CassandraTileStoreJAXB) unmarshall( JAXB_PACKAGE, CONFIG_SCHEMA, configUrl, workspace );
            
            TileMatrixSetManager mgr = workspace.getSubsystemManager( TileMatrixSetManager.class );

            Map<String, TileDataSet> map = new HashMap<String, TileDataSet>();
                        
            CassandraDB cassaDB = new CassandraDB(
                    config.getCassandraHosts(),
                    config.getCassandraKeyspace() 
            );
            
            for ( CassandraTileStoreJAXB.TileDataSet tds : config.getTileDataSet() ) {
                String id = tds.getIdentifier();
                String tmsId = tds.getTileMatrixSetId();
                
                TileMatrixSet tms = mgr.get( tmsId );
                if ( tms == null ) {
                    throw new ResourceInitException( "No tile matrix set with id " + tmsId + " is available!" );
                }
                
                List<TileDataLevel> list = new ArrayList<TileDataLevel>( tms.getTileMatrices().size() );
                
                String columnFamily = tds.getCassandraColumnfamily();
                CassandraConnector caConnector = new CassandraConnector(cassaDB, columnFamily);
                for ( TileMatrix tm : tms.getTileMatrices() ) {
                    list.add(new CassandraTileDataLevel(tm, caConnector));
                }

                DefaultTileDataSet dataset = new DefaultTileDataSet( list, tms, "image/png" );
                caConnector.setTileDataSet(dataset);
                
                map.put( id, dataset );
            }

            return new CassandraTileStore( map );
        } catch ( ResourceInitException e ) {
            throw e;
        } catch ( Throwable e ) {
            String msg = "Unable to create CassandraTileStore: " + e.getMessage();
            LOG.error( msg, e );
            throw new ResourceInitException( msg, e );
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Class<? extends ResourceManager>[] getDependencies() {
        return new Class[] {};
    }
    
    @Override
    public String getConfigNamespace() {
        return CONFIG_NAMESPACE;
    }

    @Override
    public URL getConfigSchema() {
        return CONFIG_SCHEMA;
    }    
    
    @Override
    public List<File> getTileStoreDependencies( File config ) {
        return Collections.<File> emptyList();
    }

}
