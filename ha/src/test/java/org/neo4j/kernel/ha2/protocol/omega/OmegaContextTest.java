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
package org.neo4j.kernel.ha2.protocol.omega;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.neo4j.kernel.ha2.protocol.cluster.ClusterContext;
import org.neo4j.kernel.ha2.protocol.omega.payload.CollectPayload;
import org.neo4j.kernel.ha2.protocol.omega.state.EpochNumber;
import org.neo4j.kernel.ha2.protocol.omega.state.State;
import org.neo4j.kernel.ha2.protocol.omega.state.View;

public class OmegaContextTest
{
    @Test
    public void testOrderingOfEpochNumberOnSerialNum()
    {
        List<EpochNumber> toSort = new LinkedList<EpochNumber>();
        // Creates a list in order 5,4,6,3,7,2,8,1,9
        for (int i = 1; i < 10; i++)
        {
            // The sign code is lame, but i couldn't figure a branch free way
            EpochNumber epoch = new EpochNumber(5 + ((i + 1) / 2) * (i%2 == 0 ? -1 : 1), i);
            toSort.add( epoch );
        }

        Collections.sort( toSort );

        for (int i = 1; i < toSort.size(); i++)
        {
            EpochNumber prev = toSort.get( i - 1 );
            EpochNumber current = toSort.get( i );
            assertTrue(prev.getSerialNum() < current.getSerialNum());
        }
    }

    @Test
    public void testOrderingOfEpochNumberOnProcessId()
    {
        List<EpochNumber> toSort = new LinkedList<EpochNumber>();
        // Creates a list in order 5,4,6,3,7,2,8,1,9
        for (int i = 1; i < 10; i++)
        {
            EpochNumber epoch = new EpochNumber(1, 5 + ((i + 1) / 2) * (i%2 == 0 ? -1 : 1));
            toSort.add( epoch );
        }

        Collections.sort( toSort );

        for (int i = 1; i < toSort.size(); i++)
        {
            EpochNumber prev = toSort.get( i - 1 );
            EpochNumber current = toSort.get( i );
            assertTrue(prev.getProcessId() < current.getProcessId() );
        }
    }

    @Test
    public void testOrderingEqualEpochs()
    {
        assertEquals( 0, new EpochNumber().compareTo( new EpochNumber() ) );
    }

    @Test
    public void testOrderingOfStateOnEpochNum()
    {
        List<State> toSort = new LinkedList<State>();
        // Creates a list in order 5,4,6,3,7,2,8,1,9
        for (int i = 1; i < 10; i++)
        {
            EpochNumber epoch = new EpochNumber(5 + ((i + 1) / 2) * (i%2 == 0 ? -1 : 1), 1);
            State state = new State(epoch, 1);
            toSort.add( state );
        }

        Collections.sort( toSort );

        for (int i = 1; i < toSort.size(); i++)
        {
            State prev = toSort.get( i - 1 );
            State current = toSort.get( i );
            assertTrue(prev.getEpochNum().getSerialNum() < current.getEpochNum().getSerialNum() );
        }
    }

    @Test
    public void testBasicRefreshRound()
    {
        OmegaContext context = new OmegaContext( mock( ClusterContext.class ) );
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
        OmegaContext context = new OmegaContext( mock( ClusterContext.class ) );
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

    @Test
    public void testFirstAndSecondCollectionRound() throws URISyntaxException
    {
        OmegaContext context = new OmegaContext( mock( ClusterContext.class ) );
        int firstCollectionRound = context.startCollectionRound();
        assertEquals( 1, firstCollectionRound );
        assertEquals( Collections.emptyMap(), context.getPreviousViewForCollectionRound( firstCollectionRound ) );
        assertEquals( 0, context.getStatusResponsesForRound( firstCollectionRound ) );
        context.responseReceivedForRound( firstCollectionRound );
        assertEquals( 1, context.getStatusResponsesForRound( firstCollectionRound ) );
        context.collectionRoundDone( firstCollectionRound );

        // For testing the old views in the second round
        URI responder = new URI( "neo4j://dummy" );
        View respondersView = mock( View.class );
        context.getViews().put( responder, respondersView );

        int secondCollectionRound = context.startCollectionRound();
        assertEquals( secondCollectionRound, firstCollectionRound+1 );
        assertEquals( Collections.singletonMap( responder, respondersView ), context.getPreviousViewForCollectionRound( secondCollectionRound ) );
    }
}
