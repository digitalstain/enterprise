package org.neo4j.kernel.ha2.protocol.omega;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterContext;

public class OmegaContextTest
{
    @Test
    public void testOrderingOfEpochNumberOnSerialNum()
    {
        List<OmegaContext.EpochNumber> toSort = new LinkedList<OmegaContext.EpochNumber>();
        // Creates a list in order 5,4,6,3,7,2,8,1,9
        for (int i = 1; i < 10; i++)
        {
            OmegaContext.EpochNumber epoch = new OmegaContext.EpochNumber();
            // The sign code is lame, but i couldn't figure a branch free way
            epoch.serialNum = 5 + ((i + 1) / 2) * (i%2 == 0 ? -1 : 1);
            epoch.processId = i;
            toSort.add( epoch );
        }

        Collections.sort( toSort );

        for (int i = 1; i < toSort.size(); i++)
        {
            OmegaContext.EpochNumber prev = toSort.get( i - 1 );
            OmegaContext.EpochNumber current = toSort.get( i );
            assertTrue(prev.serialNum < current.serialNum);
        }
    }

    @Test
    public void testOrderingOfEpochNumberOnProcessId()
    {
        List<OmegaContext.EpochNumber> toSort = new LinkedList<OmegaContext.EpochNumber>();
        // Creates a list in order 5,4,6,3,7,2,8,1,9
        for (int i = 1; i < 10; i++)
        {
            OmegaContext.EpochNumber epoch = new OmegaContext.EpochNumber();
            // The sign code is lame, but i couldn't figure a branch free way
            epoch.serialNum = 1;
            epoch.processId = 5 + ((i + 1) / 2) * (i%2 == 0 ? -1 : 1);
            toSort.add( epoch );
        }

        Collections.sort( toSort );

        for (int i = 1; i < toSort.size(); i++)
        {
            OmegaContext.EpochNumber prev = toSort.get( i - 1 );
            OmegaContext.EpochNumber current = toSort.get( i );
            assertTrue(prev.processId < current.processId );
        }
    }

    @Test
    public void testOrderingEqualEpochs()
    {
        assertEquals( 0, new OmegaContext.EpochNumber().compareTo( new OmegaContext.EpochNumber() ) );
    }

    @Test
    public void testOrderingOfStateOnEpochNum()
    {
        List<OmegaContext.State> toSort = new LinkedList<OmegaContext.State>();
        // Creates a list in order 5,4,6,3,7,2,8,1,9
        for (int i = 1; i < 10; i++)
        {
            OmegaContext.EpochNumber epoch = new OmegaContext.EpochNumber();
            // The sign code is lame, but i couldn't figure a branch free way
            epoch.serialNum = 5 + ((i + 1) / 2) * (i%2 == 0 ? -1 : 1);
            epoch.processId = 1;
            OmegaContext.State state = new OmegaContext.State();
            state.epochNum = epoch;
            state.freshness = 1;
        }

        Collections.sort( toSort );

        for (int i = 1; i < toSort.size(); i++)
        {
            OmegaContext.State prev = toSort.get( i - 1 );
            OmegaContext.State current = toSort.get( i );
            assertTrue(prev.epochNum.processId < current.epochNum.processId );
        }
    }

    @Test
    public void testBasicRefreshRound()
    {
        OmegaContext context = new OmegaContext( Mockito.mock( ClusterContext.class ) );
        assertEquals( -1, context.getAckCount(0) );
        int refreshRoundOne = context.startRefreshRound();
        assertEquals( 0, context.getAckCount(refreshRoundOne) );
        context.ackReceived(refreshRoundOne);
        assertEquals( 1, context.getAckCount(refreshRoundOne) );
        context.roundDone(refreshRoundOne);
        assertEquals( -1, context.getAckCount( refreshRoundOne ) );
    }

    @Test
    public void testTwoParallelRefreshRounds()
    {
        OmegaContext context = new OmegaContext( Mockito.mock( ClusterContext.class ) );
        int refreshRoundOne = context.startRefreshRound();
        context.ackReceived(refreshRoundOne);
        int refreshRoundTwo = context.startRefreshRound();
        context.ackReceived(refreshRoundOne);
        context.ackReceived(refreshRoundTwo);
        assertEquals( 2, context.getAckCount(refreshRoundOne) );
        assertEquals( 1, context.getAckCount(refreshRoundTwo) );
        context.roundDone(refreshRoundOne);
        assertEquals( -1, context.getAckCount( refreshRoundOne ) );
        assertEquals( 1, context.getAckCount(refreshRoundTwo) );
    }
}
