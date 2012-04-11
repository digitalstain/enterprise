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
package org.neo4j.backup;

import static org.neo4j.com.SlaveContext.lastAppliedTx;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.neo4j.backup.check.ConsistencyCheck;
import org.neo4j.com.Client;
import org.neo4j.com.MasterUtil;
import org.neo4j.com.MasterUtil.TxHandler;
import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.SlaveContext.Tx;
import org.neo4j.com.ToFileStoreWriter;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TxExtractor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Triplet;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchLogVersionException;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;

public class OnlineBackup
{
    private final String hostNameOrIp;
    private final int port;
    private final Map<String, Long> lastCommittedTxs = new TreeMap<String, Long>();

    public static OnlineBackup from( String hostNameOrIp, int port )
    {
        return new OnlineBackup( hostNameOrIp, port );
    }

    public static OnlineBackup from( String hostNameOrIp )
    {
        return new OnlineBackup( hostNameOrIp, BackupServer.DEFAULT_PORT );
    }

    private OnlineBackup( String hostNameOrIp, int port )
    {
        this.hostNameOrIp = hostNameOrIp;
        this.port = port;
    }

    public OnlineBackup full( String targetDirectory )
    {
        return full( targetDirectory, true );
    }

    public OnlineBackup full( String targetDirectory, boolean verification )
    {
        if ( directoryContainsDb( targetDirectory ) )
        {
            throw new RuntimeException( targetDirectory + " already contains a database" );
        }

        BackupClient client = new BackupClient( hostNameOrIp, port, StringLogger.DEV_NULL, Client.NO_STORE_ID_GETTER );
        long timestamp = System.currentTimeMillis();
        try
        {
            Response<Void> response = client.fullBackup( new ToFileStoreWriter(
                    targetDirectory ) );
            AbstractGraphDatabase targetDb = startTemporaryDb( targetDirectory,
                    VerificationLevel.NONE /* run full check instead */ );
            try
            {
                // First, receive all txs pending
                unpackResponse( response, targetDb, MasterUtil.txHandlerForFullCopy() );
                // Then go over all datasources, try to extract the latest tx
                Set<String> noTxPresent = new HashSet<String>();
                for ( XaDataSource ds : targetDb.getConfig().getTxModule().getXaDataSourceManager().getAllRegisteredDataSources() )
                {
                    long lastTx = ds.getLastCommittedTxId();
                    try
                    {
                        // This fails if the tx is not present with NSLVE
                        ds.getMasterForCommittedTx( lastTx );
                    }
                    catch ( NoSuchLogVersionException e )
                    {
                        // Note the name of the datasource
                        noTxPresent.add( ds.getName() );
                    }
                    catch ( IOException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
                if ( !noTxPresent.isEmpty() )
                {
                    /*
                     * Create a fake slave context, asking for the transactions that
                     * span the next-to-last up to the latest for each datasource
                     */
                    BackupClient recoveryClient = new BackupClient(
                            hostNameOrIp, port, targetDb.getMessageLog(),
                            Client.storeIdGetterForDb( targetDb ) );
                    Response<Void> recoveryResponse = null;
                    Map<String, Long> recoveryDiff = new HashMap<String, Long>();
                    for ( String ds : noTxPresent )
                    {
                        recoveryDiff.put( ds, -1L );
                    }
                    SlaveContext recoveryCtx = addDiffToSlaveContext(
                            slaveContextOf( targetDb ), recoveryDiff );
                    try
                    {
                        recoveryResponse = recoveryClient.incrementalBackup( recoveryCtx );
                        // Ok, the response is here, apply it.
                        TransactionStream txs = recoveryResponse.transactions();
                        ByteBuffer scratch = ByteBuffer.allocate( 64 );
                        while ( txs.hasNext() )
                        {
                            /*
                             * For each tx stream in the response, create the latest archived
                             * logical log file and write out in there the transaction.
                             *
                             */
                            Triplet<String, Long, TxExtractor> tx = txs.next();
                            scratch.clear();
                            XaDataSource ds = targetDb.getConfig().getTxModule().getXaDataSourceManager().getXaDataSource(
                                    tx.first() );
                            long logVersion = ds.getCurrentLogVersion() - 1;
                            FileChannel newLog = new RandomAccessFile(
                                    ds.getFileName( logVersion ),
                                    "rw" ).getChannel();
                            newLog.truncate( 0 );
                            LogIoUtils.writeLogHeader( scratch, logVersion, -1 );
                            // scratch buffer is flipped by writeLogHeader
                            newLog.write( scratch );
                            ReadableByteChannel received = tx.third().extract();
                            scratch.flip();
                            while ( received.read( scratch ) > 0 )
                            {
                                scratch.flip();
                                newLog.write( scratch );
                                scratch.flip();
                            }
                            newLog.force( false );
                            newLog.close();
                            received.close();
                        }
                    }
                    catch ( IOException e )
                    {
                        throw new RuntimeException( e );
                    }
                    finally
                    {
                        recoveryClient.shutdown();
                        if ( recoveryResponse != null )
                        {
                            recoveryResponse.close();
                        }
                        targetDb.shutdown();
                    }
                }
            }
            finally
            {
                targetDb.shutdown();
            }
            bumpLogFile( targetDirectory, timestamp );
            if ( verification )
            {
                StoreAccess newStore = new StoreAccess( targetDirectory );
                try
                {
                    ConsistencyCheck.run( newStore, false );
                }
                finally
                {
                    newStore.close();
                }
            }
        }
        finally
        {
            client.shutdown();
        }
        return this;
    }

    static boolean directoryContainsDb( String targetDirectory )
    {
        return new File( targetDirectory, NeoStore.DEFAULT_NAME ).exists();
    }

    public int getPort()
    {
        return port;
    }

    public String getHostNameOrIp()
    {
        return hostNameOrIp;
    }

    public Map<String, Long> getLastCommittedTxs()
    {
        return Collections.unmodifiableMap( lastCommittedTxs );
    }

    static EmbeddedGraphDatabase startTemporaryDb( String targetDirectory, VerificationLevel verification )
    {
        if ( verification != VerificationLevel.NONE ) {
            return new EmbeddedGraphDatabase( targetDirectory, MapUtil.stringMap(
                            Config.INTERCEPT_DESERIALIZED_TRANSACTIONS, "true",
                            TransactionInterceptorProvider.class.getSimpleName()
                            +"."+verification.interceptorName, verification.configValue ) );
        }
        else
            return new EmbeddedGraphDatabase( targetDirectory );
    }

    public OnlineBackup incremental( String targetDirectory )
    {
        return incremental( targetDirectory, true );
    }

    public OnlineBackup incremental( String targetDirectory, boolean verification )
    {
        if ( !directoryContainsDb( targetDirectory ) )
        {
            throw new RuntimeException( targetDirectory + " doesn't contain a database" );
        }
        GraphDatabaseService targetDb = startTemporaryDb( targetDirectory, VerificationLevel.valueOf( verification ) );

        long backupStartTime = System.currentTimeMillis();
        OnlineBackup result = null;
        try
        {
            result = incremental( targetDb );
        }
        finally
        {
            targetDb.shutdown();
        }

        /*
         * If result is not null, incremental backup was successful. It is a nice
         * idea to bump up the messages.log timestamp to reflect the latest backup
         * happened time.
         */
        if (result != null)
        {
            bumpLogFile( targetDirectory, backupStartTime );
        }
        return result;
    }

    private SlaveContext addDiffToSlaveContext( SlaveContext original,
            Map<String, Long> diffPerDataSource )
    {
        Tx[] oldTxs = original.lastAppliedTransactions();
        Tx[] newTxs = new Tx[oldTxs.length];
        for ( int i = 0; i < oldTxs.length; i++ )
        {
            Tx oldTx = oldTxs[i];
            String dsName = oldTx.getDataSourceName();
            long originalTxId = oldTx.getTxId();
            Long diff = diffPerDataSource.get( dsName );
            if ( diff == null )
            {
                diff = Long.valueOf( 0L );
            }
            long newTxId = originalTxId + diff;
            newTxs[i] = SlaveContext.lastAppliedTx( dsName, newTxId );
        }
        return SlaveContext.anonymous( newTxs );
    }

    /**
     * Performs an incremental backup based off the given context. This means
     * receiving and applying selectively (i.e. irrespective of the actual state
     * of the target db) a set of transactions starting at the desired txId and
     * spanning up to the latest of the master, for every data source
     * registered.
     *
     * @param targetDb The database that contains a previous full copy
     * @param context The context, i.e. a mapping of data source name to txid
     *            which will be the first in the returned stream
     * @return A backup context, ready to perform
     */
    private OnlineBackup incrementalWithContext( GraphDatabaseService targetDb,
            SlaveContext context )
    {
        BackupClient client = new BackupClient( hostNameOrIp, port, ((AbstractGraphDatabase)targetDb).getMessageLog(),
                Client.storeIdGetterForDb( targetDb ) );
        try
        {
            unpackResponse( client.incrementalBackup( slaveContextOf( targetDb ) ), targetDb, MasterUtil.NO_ACTION );
        }
        finally
        {
            client.shutdown();
        }
        return this;
    }

    public OnlineBackup incremental( GraphDatabaseService targetDb )
    {
        return incrementalWithContext( targetDb, slaveContextOf( targetDb ) );
    }

    private void unpackResponse( Response<Void> response, GraphDatabaseService graphDb, TxHandler txHandler )
    {
        try
        {
            MasterUtil.applyReceivedTransactions( response, graphDb, txHandler );
            getLastCommittedTxs( graphDb );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to apply received transactions", e );
        }
    }

    private void getLastCommittedTxs( GraphDatabaseService graphDb )
    {
        for ( XaDataSource ds : ((AbstractGraphDatabase) graphDb).getConfig().getTxModule().getXaDataSourceManager().getAllRegisteredDataSources() )
        {
            lastCommittedTxs.put( ds.getName(), ds.getLastCommittedTxId() );
        }
    }

    private SlaveContext slaveContextOf( GraphDatabaseService graphDb )
    {
        XaDataSourceManager dsManager =
                ((AbstractGraphDatabase) graphDb).getConfig().getTxModule().getXaDataSourceManager();
        List<Tx> txs = new ArrayList<Tx>();
        for ( XaDataSource ds : dsManager.getAllRegisteredDataSources() )
        {
            txs.add( lastAppliedTx( ds.getName(), ds.getLastCommittedTxId() ) );
        }
        return SlaveContext.anonymous( txs.toArray( new Tx[0] ) );
    }

    private static boolean bumpLogFile( String targetDirectory, long toTimestamp )
    {
        File dbDirectory = new File( targetDirectory );
        File[] candidates = dbDirectory.listFiles( new FilenameFilter()
        {
            @Override
            public boolean accept( File dir, String name )
            {
                /*
                 *  Contains ensures that previously timestamped files are
                 *  picked up as well
                 */
                return name.equals( StringLogger.DEFAULT_NAME );
            }
        } );
        File previous = null;
        if ( candidates.length != 1 )
        {
            return false;
        }
        // candidates has a unique member, the right one
        else
        {
            previous = candidates[0];
        }
        // Build to, from existing parent + new filename
        File to = new File( previous.getParentFile(), StringLogger.DEFAULT_NAME
                                                      + "." + toTimestamp );
        return previous.renameTo( to );
    }
}
