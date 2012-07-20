/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.haonefive;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.neo4j.com.ConnectionLostHandler;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.RequestContext.Tx;
import org.neo4j.com.ToFileStoreWriter;
import org.neo4j.com.TxChecksumVerifier;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.BranchedDataPolicy;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterClient18;
import org.neo4j.kernel.ha.MasterClientResolver;
import org.neo4j.kernel.ha.MasterImpl;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.core.RelationshipTypeCreator;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class HaOneFiveGraphDb extends InternalAbstractGraphDatabase implements MasterChangeListener
{
    private static final int DEFAULT_STATE_SWITCH_TIMEOUT = 20;
    
    final ComRequestSupport requestSupport;
    final TransactionSupport transactionSupport;
    private final int serverId;
    private volatile long sessionTimestamp;
    private volatile long lastUpdated;
    
    private volatile Master master;
    private volatile int masterServerId = -1;
    private volatile DatabaseState databaseState = DatabaseState.TBD;
    private volatile MasterServer server;
    private MasterElectionClient masterElectionClient;
    private final StateSwitchBlock switchBlock = new StateSwitchBlock();
    
    // TODO configurable
    private final int stateSwitchTimeout = DEFAULT_STATE_SWITCH_TIMEOUT;
    
    public HaOneFiveGraphDb( String storeDir, Map<String, String> params )
    {
        super( storeDir, (params = withDefaults( params )), Service.load( IndexProvider.class ),
                Service.load( KernelExtension.class ), Service.load( CacheProvider.class ) );
        
        serverId = new Config( params ).getInteger( HaSettings.server_id );
        sessionTimestamp = System.currentTimeMillis();
        requestSupport = new ComRequestSupport()
        {
            @Override
            public void receive( Response<?> response )
            {
                try
                {
                    ServerUtil.applyReceivedTransactions( response, HaOneFiveGraphDb.this, ServerUtil.NO_ACTION );
                    lastUpdated = System.currentTimeMillis();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
            
            @Override
            public RequestContext getRequestContext()
            {
                return getRequestContext( txManager.getEventIdentifier() );
            }
            
            @Override
            public RequestContext getRequestContext( XaDataSource dataSource )
            {
                return ServerUtil.getRequestContext( dataSource, sessionTimestamp, serverId,
                        txManager.getEventIdentifier() );
            }
            
            @Override
            public RequestContext getRequestContext( int eventIdentifier )
            {
                return ServerUtil.getRequestContext( xaDataSourceManager, sessionTimestamp, serverId, eventIdentifier );
            }
        };
        
        transactionSupport = new TransactionSupport()
        {
            @Override
            public boolean hasAnyLocks( Transaction tx )
            {
                return lockReleaser.hasLocks( tx );
            }
            
            @Override
            public void makeSureTxHasBeenInitialized()
            {
                try
                {
                    Transaction tx = txManager.getTransaction();
                    int eventIdentifier = txManager.getEventIdentifier();
                    if ( !hasAnyLocks( tx ) )
                    {
                        txHook.initializeTransaction( eventIdentifier );
                        initializeTx();
                    }
                }
                catch ( SystemException e )
                {
                    throw new RuntimeException( e );
                }
            }

//            @Override
//            public int getMasterIdForTx( long tx )
//            {
//                try
//                {
//                    return getXaDataSourceManager().getNeoStoreDataSource().getMasterForCommittedTx( tx ).first().intValue();
//                }
//                catch ( IOException e )
//                {
//                    throw new RuntimeException( e );
//                }
//            }
        };
        
        run();
    }
    
    protected void create()
    {
        masterElectionClient = life.add( createMasterElectionClient() );
        masterElectionClient.addMasterChangeListener( this );
    }
    
    public boolean isMaster()
    {
        return databaseState == DatabaseState.MASTER;
    }
    
    private Triplet<Long, Integer, Long> getLastTx()
    {
        try
        {
            NeoStoreXaDataSource neoStoreDataSource = getXaDataSourceManager().getNeoStoreDataSource();
            long tx = neoStoreDataSource.getLastCommittedTxId();
            Pair<Integer, Long> gottenMaster = neoStoreDataSource.getMasterForCommittedTx( tx );
            return Triplet.of( tx, gottenMaster.first(), gottenMaster.other() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    private static Map<String, String> withDefaults( Map<String, String> params )
    {
        params = new ConfigurationDefaults( HaSettings.class ).apply( params );
        params.put( Config.KEEP_LOGICAL_LOGS, "true" );
        return params;
    }

    protected MasterElectionClient createMasterElectionClient()
    {
        return new ZooKeeperMasterElectionClient( requestSupport, config, storeId, storeDir,
                new MasterClientResolver( getMessageLog(), config.get( HaSettings.read_timeout ),
                        config.get( HaSettings.lock_read_timeout ),
                        config.get( HaSettings.max_concurrent_channels_per_slave ) ) )
        {
            @Override
            public int getMasterForTx( long tx )
            {
                try
                {
                    return getXaDataSourceManager().getNeoStoreDataSource().getMasterForCommittedTx( tx ).first()
                            .intValue();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
    }
    
    @Override
    public org.neo4j.graphdb.Transaction beginTx()
    {
        // TODO first startup ever we don't have a proper db, so don't even serve read requests
        // if this is a startup for where we have been a member of this cluster before we
        // can server (possibly quite outdated) read requests.
        return super.beginTx();
    }
    
    protected void initializeTx()
    {
        if ( !switchBlock.await( stateSwitchTimeout ) )
            // TODO Specific exception instead?
            throw new RuntimeException( "Timed out waiting for database to switch state" );
    }

    @Override
    public void shutdown()
    {
        databaseState.shutdown( this );
//        masterElectionClient.shutdown();
        super.shutdown();
    }
    
    @Override
    protected TxHook createTxHook()
    {
        return new HaTxHook( requestSupport, transactionSupport );
    }
    
    @Override
    protected TxIdGenerator createTxIdGenerator()
    {
        return new HaTxIdGenerator( requestSupport, serverId );
    }
    
    @Override
    protected IdGeneratorFactory createIdGeneratorFactory()
    {
        return new HaIdGeneratorFactory( serverId );
    }
    
    @Override
    protected LockManager createLockManager()
    {
        return new HaLockManager( requestSupport, transactionSupport, ragManager );
    }
    
    @Override
    protected RelationshipTypeCreator createRelationshipTypeCreator()
    {
        return new HaRelationshipTypeCreator( requestSupport );
    }

    @Override
    public void newMasterElected( URL masterUrl, int masterServerId )
    {
        if ( this.masterServerId == masterServerId )
            return;
        
        enterDatabaseStateSwitchBlockade();
        
        boolean iAmToBecomeMaster = masterServerId == this.serverId;
        DatabaseState newState = databaseState;
        if ( iAmToBecomeMaster )
        {
            newState = databaseState.becomeMaster( this, masterUrl.getPort() );
        }
        else
        {
            newState = databaseState.becomeSlave( this, masterUrl.getHost(), masterUrl.getPort() );
        }
        this.masterServerId = masterServerId;
        ((HaIdGeneratorFactory) idGeneratorFactory).masterChanged( master, masterServerId );
        ((HaTxIdGenerator) txIdGenerator).masterChanged( master, masterServerId );
        ((HaTxHook) txIdGenerator).masterChanged( master );
        ((HaLockManager) txIdGenerator).masterChanged( master );
        
        databaseState = newState;
    }

    @Override
    public void newMasterBecameAvailable( URL masterUrl )
    {
        databaseState.verifyConsistencyWithMaster( this );
        
        exitDatabaseStateSwitchBlockade();
    }
    
    private void enterDatabaseStateSwitchBlockade()
    {
        // TODO Block incoming write transactions and roll back active write transactions or something.
        switchBlock.enter();
    }

    private void exitDatabaseStateSwitchBlockade()
    {
        // TODO
        switchBlock.exit();
    }
    
    public void pullUpdates()
    {
        Response<Void> response = master.pullUpdates( requestSupport.getRequestContext() );
        requestSupport.receive( response );
    }

    enum DatabaseState
    {
        TBD
        {
            @Override
            void handleWriteOperation( HaOneFiveGraphDb db )
            {
                waitForRoleDecision( db );
            }

            @Override
            DatabaseState becomeMaster( HaOneFiveGraphDb db, int port )
            {
                db.server = newServer( db, port );
                db.master = newLoopbackMaster( db );
                return MASTER;
            }

            @Override
            DatabaseState becomeSlave( final HaOneFiveGraphDb db, String masterIp, int masterPort )
            {
                db.master = newClient( db, masterIp, masterPort );
                
                // If my db is empty than the master then copy it from the master. This happens
                // when we start up for the first time and the AbstractGraphDatabase constructor
                // creates an empty db.
                if ( dbIsEmpty( db ) )
                    db.copyStore( BranchedDataPolicy.keep_none );
                return SLAVE;
            }
            
            @Override
            DatabaseState becomeUndecided( HaOneFiveGraphDb db )
            {
                return this;
            }

            @Override
            void beforeGetMaster( HaOneFiveGraphDb db )
            {
                waitForRoleDecision( db );
            }
            
            @Override
            void shutdown( HaOneFiveGraphDb db )
            {
            }

            @Override
            void verifyConsistencyWithMaster( HaOneFiveGraphDb db )
            {
                throw new UnsupportedOperationException( "Should not be called" );
            }
        },
        MASTER
        {
            @Override
            void handleWriteOperation( HaOneFiveGraphDb db )
            {
            }

            @Override
            DatabaseState becomeMaster( HaOneFiveGraphDb db, int port )
            {
                // Do nothing, I'm already master
                return MASTER;
            }

            @Override
            DatabaseState becomeSlave( HaOneFiveGraphDb db, String masterIp, int masterPort )
            {
                // TODO Switch to slave
                db.server.shutdown();
                db.server = null;
                db.master.shutdown();
                db.master = newClient( db, masterIp, masterPort );
                return SLAVE;
            }
            
            @Override
            DatabaseState becomeUndecided( HaOneFiveGraphDb db )
            {
                db.server.shutdown();
                db.server = null;
                db.master.shutdown();
                return TBD;
            }

            @Override
            void beforeGetMaster( HaOneFiveGraphDb db )
            {
            }
            
            @Override
            void shutdown( HaOneFiveGraphDb db )
            {
                db.server.shutdown();
            }

            @Override
            void verifyConsistencyWithMaster( HaOneFiveGraphDb db )
            {
            }
        },
        SLAVE
        {
            @Override
            void handleWriteOperation( HaOneFiveGraphDb db )
            {
            }

            @Override
            DatabaseState becomeMaster( HaOneFiveGraphDb db, int port )
            {
                db.server = newServer( db, port );
                db.master.shutdown();
                db.master = newLoopbackMaster( db );
                return MASTER;
            }

            @Override
            DatabaseState becomeSlave( HaOneFiveGraphDb db, String masterIp, int masterPort )
            {
                db.master.shutdown();
                db.master = newClient( db, masterIp, masterPort );
                return SLAVE;
            }
            
            @Override
            DatabaseState becomeUndecided( HaOneFiveGraphDb db )
            {
                db.master.shutdown();
                return TBD;
            }

            @Override
            void beforeGetMaster( HaOneFiveGraphDb db )
            {
            }
            
            @Override
            void shutdown( HaOneFiveGraphDb db )
            {
            }

            @Override
            void verifyConsistencyWithMaster( HaOneFiveGraphDb db )
            {
                db.verifyConsistencyWithMaster();
            }
        };
        
        abstract DatabaseState becomeMaster( HaOneFiveGraphDb db, int port );
        
        protected void waitForRoleDecision( HaOneFiveGraphDb db )
        {
            // Wait for a master/slave decision some time and see if a decision is made.
            // If no decisions was made before the timeout then throw exception.
            long endTime = currentTimeMillis() + SECONDS.toMillis( 20 );
            try
            {
                while ( currentTimeMillis() < endTime )
                {
                    if ( db.databaseState != this )
                        return;
                    Thread.sleep( 10 );
                }
                throw new RuntimeException( "No role decision was made" );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                throw new RuntimeException( e );
            }
        }

        protected boolean dbIsEmpty( HaOneFiveGraphDb db )
        {
            return db.getXaDataSourceManager().getNeoStoreDataSource().getNeoStore().getLastCommittedTx() == 1;
        }

        abstract DatabaseState becomeSlave( HaOneFiveGraphDb db, String masterIp, int masterPort );
        
        abstract DatabaseState becomeUndecided( HaOneFiveGraphDb db );
        
        abstract void beforeGetMaster( HaOneFiveGraphDb db );
        
        protected Master newLoopbackMaster( HaOneFiveGraphDb db )
        {
            return new LoopbackMaster( db.storeId, db.xaDataSourceManager,
                    db.txManager, db.persistenceSource, db.persistenceManager, db.relationshipTypeHolder );
        }

        protected MasterServer newServer( HaOneFiveGraphDb db, int port )
        {
            return new MasterServer( new MasterImpl( db, 20 ), port, db.getMessageLog(),
                    20, 20, TxChecksumVerifier.ALWAYS_MATCH );
        }

        protected Master newClient( HaOneFiveGraphDb db, String masterIp, int masterPort )
        {
            // TODO Wrap returned Master in something that handles exceptions (network a.s.o.)
            // and feeds back to master election black box if we decide to have input channels to it.
            return new MasterClient18( masterIp, masterPort, db.getMessageLog(), db.getStoreId(),
                    new ConnectionFailureHandler( db ), 20, 20, 20 );
        }

        abstract void handleWriteOperation( HaOneFiveGraphDb db );
        
        abstract void shutdown( HaOneFiveGraphDb db );
        
        abstract void verifyConsistencyWithMaster( HaOneFiveGraphDb db );
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + serverId + ", " + storeDir + "]";
    }
    
    private void copyStore( BranchedDataPolicy policy )
    {
        life.stop();
        try
        {
            policy.handle( this );
            RequestContext context = new RequestContext( 0, serverId, 0, new Tx[0], 0, 0 );
            Response<Void> response = master.copyStore( context, new ToFileStoreWriter( storeDir ) );
            requestSupport.receive( response );
        }
        finally
        {
            life.start();
        }
    }

    private void verifyConsistencyWithMaster()
    {
        Triplet<Long, Integer, Long> myLastTx = getLastTx();
        boolean okWithMaster = false;
        try
        {
            Pair<Integer, Long> mastersLastTx = master.getMasterIdForCommittedTx( myLastTx.first(), storeId ).response();
            okWithMaster = myLastTx.other().equals( mastersLastTx );
        }
        catch ( RuntimeException e )
        {
            // Maybe master is missing transactions that I have... looks like branched data to me.
        }
        
        if ( !okWithMaster )
            copyStore( BranchedDataPolicy.keep_all );
        // We're good to go
    }

    private static class ConnectionFailureHandler implements ConnectionLostHandler
    {
        private final HaOneFiveGraphDb db;

        ConnectionFailureHandler( HaOneFiveGraphDb db )
        {
            this.db = db;
        }
        
        @Override
        public void handle( Exception e )
        {
            db.enterDatabaseStateSwitchBlockade();
            db.databaseState = db.databaseState.becomeUndecided( db );
//            db.masterElectionClient.masterCommunicationFailed();
        }
    }
}
