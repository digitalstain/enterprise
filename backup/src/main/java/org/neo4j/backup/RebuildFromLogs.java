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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import javax.transaction.xa.Xid;
import org.neo4j.backup.check.ConsistencyCheck;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.ProgressIndicator;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigParam;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.nioneo.xa.Command;
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogExtractor;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

import static org.neo4j.helpers.ProgressIndicator.SimpleProgress.*;
import static org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource.*;
import static org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog.*;

class RebuildFromLogs
{
    /*
     * TODO: This process can be sped up if the target db doesn't have to write tx logs.
     */
    private final XaDataSource nioneo;
    private final StoreAccess stores;

    RebuildFromLogs( AbstractGraphDatabase graphdb )
    {
        this.nioneo = getDataSource( graphdb, Config.DEFAULT_DATA_SOURCE_NAME );
        this.stores = new StoreAccess( graphdb );
    }

    RebuildFromLogs applyTransactionsFrom( ProgressIndicator progress, File sourceDir ) throws IOException
    {
        LogExtractor extractor = null;
        try
        {
            extractor = LogExtractor.from( sourceDir.getAbsolutePath() );
            for ( InMemoryLogBuffer buffer = new InMemoryLogBuffer();; buffer.reset() )
            {
                long txId = extractor.extractNext( buffer );
                if ( txId == -1 ) break;
                applyTransaction( txId, buffer );
                if ( progress != null ) progress.update( false, txId );
            }
        }
        finally
        {
            if ( extractor != null ) extractor.close();
        }
        return this;
    }

    public void applyTransaction( long txId, ReadableByteChannel txData ) throws IOException
    {
        nioneo.applyCommittedTransaction( txId, txData );
    }

    private static XaDataSource getDataSource( AbstractGraphDatabase graphdb, String name )
    {
        XaDataSource datasource = graphdb.getXaDataSourceManager().getXaDataSource( name );
        if ( datasource == null ) throw new NullPointerException( "Could not access " + name );
        return datasource;
    }

    public static void main( String[] args )
    {
        if ( args == null )
        {
            printUsage();
            return;
        }
        Args params = new Args( args );
        @SuppressWarnings( "boxing" )
        boolean full = params.getBoolean( "full", false, true );
        args = params.orphans().toArray( new String[0] );
        if ( args.length != 2 )
        {
            printUsage( "Exactly two positional arguments expected: "
                        + "<source dir with logs> <target dir for graphdb>, got " + args.length );
            System.exit( -1 );
            return;
        }
        File source = new File( args[0] ), target = new File( args[1] );
        if ( !source.isDirectory() )
        {
            printUsage( source + " is not a directory" );
            System.exit( -1 );
            return;
        }
        if ( target.exists() )
        {
            if ( target.isDirectory() )
            {
                if ( OnlineBackup.directoryContainsDb( target.getAbsolutePath() ) )
                {
                    printUsage( "target graph database already exists" );
                    System.exit( -1 );
                    return;
                }
                else
                {
                    System.err.println( "WARNING: the directory " + target + " already exists" );
                }
            }
            else
            {
                printUsage( target + " is a file" );
                System.exit( -1 );
                return;
            }
        }
        long maxFileId = findMaxLogFileId( source );
        if ( maxFileId < 0 )
        {
            printUsage( "Inconsistent number of log files found in " + source );
            System.exit( -1 );
            return;
        }
        long txCount = findLastTransactionId( source, LOGICAL_LOG_DEFAULT_NAME + ".v" + maxFileId );
        String txdifflog = params.get( "txdifflog", null, new File( target, "txdiff.log" ).getAbsolutePath() );
        AbstractGraphDatabase graphdb = OnlineBackup.startTemporaryDb( target.getAbsolutePath(),
                                                                       new TxDiffLogConfig( full
                                                                               ? VerificationLevel.FULL_WITH_LOGGING
                                                                               : VerificationLevel.LOGGING, txdifflog ) );

        ProgressIndicator progress;
        if ( txCount < 0 )
        {
            progress = null;
            System.err.println( "Unable to report progress, cannot find highest txId, attempting rebuild anyhow." );
        }
        else
        {
            progress = textual( System.err, txCount );
            System.err.printf( "Rebuilding store from %s transactions %n", Long.valueOf( txCount ) );
        }
        try
        {
            try
            {
                RebuildFromLogs rebuilder = new RebuildFromLogs( graphdb ).applyTransactionsFrom( progress, source );
                if ( progress != null ) progress.done( txCount );
                // if we didn't run the full checker for each transaction, run it afterwards
                if ( !full ) rebuilder.checkConsistency();
            }
            finally
            {
                graphdb.shutdown();
            }
        }
        catch ( IOException e )
        {
            System.err.println();
            e.printStackTrace( System.err );
            System.exit( -1 );
            return;
        }
    }

    private static long findLastTransactionId( File storeDir, String logFileName )
    {
        long txId;
        try
        {
            FileChannel channel = new RandomAccessFile( new File( storeDir, logFileName ), "r" ).getChannel();
            try
            {
                ByteBuffer buffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE + Xid.MAXBQUALSIZE * 10 );
                txId = LogIoUtils.readLogHeader( buffer, channel, true )[1];
                XaCommandFactory cf = new CommandFactory();
                for ( LogEntry entry; ( entry = LogIoUtils.readEntry( buffer, channel, cf ) ) != null; )
                {
                    if ( entry instanceof LogEntry.Commit )
                    {
                        txId = ( (LogEntry.Commit) entry ).getTxId();
                    }
                }
            }
            finally
            {
                if ( channel != null ) channel.close();
            }
        }
        catch ( IOException e )
        {
            return -1;
        }
        return txId;
    }

    private void checkConsistency()
    {
        try
        {
            ConsistencyCheck.run( stores, true );
        }
        catch ( AssertionError summary )
        {
            System.err.println( summary.getMessage() );
        }
    }

    private static void printUsage( String... msgLines )
    {
        for ( String line : msgLines ) System.err.println( line );
        System.err.println( Args.jarUsage( RebuildFromLogs.class, "[-full] <source dir with logs> <target dir for graphdb>" ) );
        System.err.println( "WHERE:   <source dir>  is the path for where transactions to rebuild from are stored" );
        System.err.println( "         <target dir>  is the path for where to create the new graph database" );
        System.err.println( "         -full     --  to run a full check over the entire store for each transaction" );
    }

    private static long findMaxLogFileId( File source )
    {
        return getHighestHistoryLogVersion( source, LOGICAL_LOG_DEFAULT_NAME );
    }

    private static class TxDiffLogConfig implements ConfigParam
    {
        private final String targetFile;
        private final VerificationLevel level;

        TxDiffLogConfig( VerificationLevel level, String targetFile )
        {
            this.level = level;
            this.targetFile = targetFile;
        }

        @Override
        public void configure( Map<String, String> config )
        {
            if ( targetFile != null )
            {
                level.configureWithDiffLog( config, targetFile );
            }
            else
            {
                level.configure( config );
            }
        }
    }

    private static class CommandFactory extends XaCommandFactory
    {
        @Override
        public XaCommand readCommand( ReadableByteChannel byteChannel, ByteBuffer buffer ) throws IOException
        {
            return Command.readCommand( null, byteChannel, buffer );
        }
    }
}
