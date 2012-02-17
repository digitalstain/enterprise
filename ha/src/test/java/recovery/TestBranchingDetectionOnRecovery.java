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
package recovery;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.HAGraphDb;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.impl.nioneo.xa.Command;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.BreakPoint.Event;
import org.neo4j.test.subprocess.BreakpointHandler;
import org.neo4j.test.subprocess.BreakpointTrigger;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.DebuggedThread;
import org.neo4j.test.subprocess.EnabledBreakpoints;
import org.neo4j.test.subprocess.ForeignBreakpoints;
import org.neo4j.test.subprocess.SubProcessTestRunner;

import slavetest.TestStoreCopy;

@ForeignBreakpoints( {
        @ForeignBreakpoints.BreakpointDef( type = "org.neo4j.kernel.impl.transaction.xaframework.XaResourceHelpImpl", method = "commit", on = Event.ENTRY ),
        @ForeignBreakpoints.BreakpointDef( type = "org.neo4j.kernel.ha.SlaveTxIdGenerator", method = "generate", on = Event.EXIT ) } )
@RunWith( SubProcessTestRunner.class )
public class TestBranchingDetectionOnRecovery
{
    private static final int TX_COUNT = 1;
    private static final List<DebuggedThread> loosers = new ArrayList<DebuggedThread>(
            TX_COUNT );
    private static DebuggedThread winner;
    private static LocalhostZooKeeperCluster zoo;

    private static CountDownLatch commitLatch = new CountDownLatch( TX_COUNT );
    private static CountDownLatch winnerDone = new CountDownLatch( 1 );

    @BeforeClass
    public static void startZoo()
    {
        zoo = new LocalhostZooKeeperCluster( TestStoreCopy.class, new int[] {
                3184, 3185, 3186 } );
        master = new HAGraphDb( TargetDirectory.forTest(
                TestBranchingDetectionOnRecovery.class ).directory( "master",
                true ).getAbsolutePath(), MapUtil.stringMap(
                HaConfig.CONFIG_KEY_COORDINATORS, zoo.getConnectionString(),
                HaConfig.CONFIG_KEY_SERVER_ID, "1" ) );
        slave = new HAGraphDb( TargetDirectory.forTest(
                TestBranchingDetectionOnRecovery.class ).directory( "slave1",
                true ).getAbsolutePath(), MapUtil.stringMap(
                HaConfig.CONFIG_KEY_COORDINATORS, zoo.getConnectionString(),
                HaConfig.CONFIG_KEY_SERVER_ID, "2" ) );
        Transaction tx = master.beginTx();
        try
        {
            Node node = master.createNode();
            master.index().forNodes( "index" ).add( node, "key", "value" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    @AfterClass
    public static void stopZoo()
    {
        zoo.shutdown();
    }

    @BreakpointTrigger( "threadsStarted" )
    private void threadsStarted()
    {
        // just trigger the bp
    }

    @BreakpointHandler( "threadsStarted" )
    public static void onThreadsStarted( BreakPoint self, DebugInterface di,
            @BreakpointHandler( "commit" ) BreakPoint commitBP )
    {
        System.out.println( "On Threads started" );
        try
        {
            commitLatch.await();
        }
        catch ( InterruptedException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        commitBP.disable();
        self.disable();
    }

    @BreakpointHandler( "commit" )
    public static void onCommit( BreakPoint self, DebugInterface di )
    {
        System.out.println( "On commit" );
        di.thread().printStackTrace( System.out );
        loosers.add( di.thread().suspend( null ) );
        commitLatch.countDown();
    }

    @BreakpointHandler( "generate" )
    public static void onGenerate( BreakPoint self, DebugInterface di )
    {
        System.out.println( "On generate" );
        di.thread().printStackTrace( System.out );
        winner = di.thread().suspend( null );
        for ( DebuggedThread dt : loosers )
        {
            System.out.println( "Another looser is " + dt.name() );
        }
        winnerDone.countDown();
    }

    private static class CommandFactory extends XaCommandFactory
    {
        @Override
        public XaCommand readCommand( ReadableByteChannel byteChannel,
                ByteBuffer buffer ) throws IOException
        {
            return Command.readCommand( null, byteChannel, buffer );
        }
    }

    @BreakpointTrigger( "waitSuspended" )
    private void waitSuspended()
    {
    }

    @BreakpointTrigger( "resumeAndWaitDead" )
    private void resumeAndWaitDead()
    {

    }

    @BreakpointHandler( "waitSuspended" )
    public static void onWaitSuspended( BreakPoint self, DebugInterface di )
    {
        System.out.println( "on wait suspended" );
        try
        {
            winnerDone.await();
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
        }
    }

    @BreakpointHandler( "resumeAndWaitDead" )
    public static void onResumeAndWaitDead()
    {
        System.out.println( "On resume and wait dead" );
        for ( DebuggedThread dt : loosers )
        {
            System.out.println( "Another looser is " + dt.name() );
            dt.resume();
        }
        winner.resume();
    }

    @BreakpointTrigger( "enableBreakpoints" )
    private void enableBreakpoints()
    {
        // just do it
    }

    @BreakpointHandler( "enableBreakpoints" )
    public static void onEnableBreakpoints( BreakPoint self, DebugInterface di,
            @BreakpointHandler( "commit" ) BreakPoint commitBP )
    {
        System.out.println( "Enabling breakpoints" );
        commitBP.enable();
    }

    private static HAGraphDb master;
    private static HAGraphDb slave;

    @Test
    @EnabledBreakpoints( { "enableBreakpoints", "generate", "threadsStarted",
            "waitSuspended", "resumeAndWaitDead" } )
    public void mainTest() throws Exception
    {
        /*
         *  Enable onCommit othewise it is triggered on master on the index
         *  creation.
         */
        enableBreakpoints();
        /*
         * Those threads are doomed to fail on commit - but since they are
         * 2PC they are prepared before that so on recovery they will be
         * committed - the master will never hear from them though since
         * the slaveTxIdGenerator will not be called.
         */
        List<Thread> theThreads = new ArrayList<Thread>( TX_COUNT );
        for ( int i = 0; i < TX_COUNT; i++ )
        {
            Thread t = new Thread( new Runnable()
            {
                @Override
                public void run()
                {
                    Transaction tx = slave.beginTx();
                    try
                    {
                        Node node = slave.createNode();
                        slave.index().forNodes( "index" ).add( node, "key",
                                "value" );
                        tx.success();
                    }
                    finally
                    {
                        tx.finish();
                    }
                }
            }, "slave thread #" + i );
            theThreads.add( t );
            t.start();
        }
        Thread.sleep( 4000 );
        threadsStarted(); // disables commit breakpoint for winner to
                          // fall through
        /*
         * This thread on the other hand will manage to generate a txid
         * on the master and transfer itself over, so it will actually
         * be there - and it will also be the last one so it will be committed
         * as the last one on recovery so it will match the checksum check
         * on verifyConsistencyWithMaster
         */
        Thread winner = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                Transaction tx = slave.beginTx();
                try
                {
                    Node node = slave.createNode();
                    slave.index().forNodes( "index" ).add( node, "key", "value" );
                    tx.success();
                }
                finally
                {
                    tx.finish();
                }
            }
        }, "winner" );
        winner.start();
        // wait for everything to finish
        Thread.sleep( 4000 );
        waitSuspended();
        /*
         *  shutdown the slave - the pending threads will simply fail to commit, so
         *  this pretty much amounts to a slave crash
         */
        slave.shutdown();
        // resume the suspended subprocesses and join the threads to exit
        // properly
        resumeAndWaitDead();
        for ( Thread t : theThreads )
        {
            t.join();
        }
        winner.join();

        /*
         *  This is to cover up the missing txid on the master for each of the
         *  onCommit killed threads, otherwise the bug will trigger a "commit entry
         *  not found for txid <blah>" which is less obvious.
         */
        Transaction tx = master.beginTx();
        for ( int i = 0; i < TX_COUNT; i++ )
        {
            master.createNode();
        }
        tx.success();
        tx.finish();

        /*
         *  Recover on HA mode, since recovering as standalone is cheating
         *  and not supported
         */
        new HAGraphDb( TargetDirectory.forTest(
                TestBranchingDetectionOnRecovery.class ).directory( "slave1",
                false ).getAbsolutePath(), MapUtil.stringMap(
                HaConfig.CONFIG_KEY_COORDINATORS, zoo.getConnectionString(),
                HaConfig.CONFIG_KEY_SERVER_ID, "2" ) ).shutdown();

        // Shutdown the master too, to start the embedded later on
        master.shutdown();

        /*
         * It is stupid to test while in HA mode since the error is now there and
         * starting HA instances makes no difference, plus it is slower.
         */
        EmbeddedGraphDatabase masterEmbedded = new EmbeddedGraphDatabase(
                TargetDirectory.forTest( TestBranchingDetectionOnRecovery.class ).directory(
                        "master", false ).getAbsolutePath() );
        EmbeddedGraphDatabase slaveEmbedded = new EmbeddedGraphDatabase(
                TargetDirectory.forTest( TestBranchingDetectionOnRecovery.class ).directory(
                        "slave1", false ).getAbsolutePath() );

        /*
         * This is the verification phase. The contents should be the same since the slave
         * was successfully recovered with the master running.
         */
        for ( Node n : slaveEmbedded.getAllNodes() )
        {
            assertEquals( "value",
                    masterEmbedded.getNodeById( n.getId() ).getProperty( "key" ) );
        }

        masterEmbedded.shutdown();
        slaveEmbedded.shutdown();
    }
}
