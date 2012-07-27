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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.kernel.ha2.protocol.omega.payload.RefreshAckPayload;
import org.neo4j.kernel.ha2.protocol.omega.payload.RefreshPayload;
import org.neo4j.kernel.ha2.protocol.omega.state.EpochNumber;
import org.neo4j.kernel.ha2.protocol.omega.state.State;
import org.neo4j.kernel.ha2.protocol.omega.state.View;

public class OmegaStateTest
{
    @Test
    public void testStartTransition() throws Throwable
    {
        OmegaContext context = mock( OmegaContext.class );
        Message<OmegaMessage> message = Message.internal( OmegaMessage.start);
        MessageProcessor outgoing = mock( MessageProcessor.class );
        OmegaState result = (OmegaState) OmegaState.start.handle( context, message, outgoing );
        // Assert we move to operational state
        assertEquals(OmegaState.omega, result);
        // And that timers started
        verify(context).startTimers();
    }

    @Test
    public void testRefreshTimeoutResponse() throws Throwable
    {
        OmegaContext context = mock( OmegaContext.class );
        Message<OmegaMessage> message = Message.internal( OmegaMessage.refresh_timeout );
        MessageProcessor outgoing = mock( MessageProcessor.class );

        State state = new State( new EpochNumber() );
        when( context.getMyState() ).thenReturn( state );

        Set<URI> servers = new HashSet<URI>();
        servers.add(new URI("localhost:80"));
        servers.add(new URI("localhost:81"));
        when( context.getServers() ).thenReturn( (Collection)servers );

        OmegaState result = (OmegaState) OmegaState.omega.handle( context, message, outgoing );

        assertEquals( OmegaState.omega, result );
        verify( context ).getServers();
        verify( outgoing, times( servers.size() ) ).process( isA( Message.class ) );
        verify( context ).startRefreshRound();
    }

    @Test
    public void testRefreshSuccess() throws Throwable
    {
        OmegaContext context = mock( OmegaContext.class );
        Message<OmegaMessage> message = Message.internal( OmegaMessage.refresh_ack, RefreshAckPayload.forRefresh( new
                RefreshPayload( 1, 2, 3, 1 ) ) );
        MessageProcessor outgoing = mock( MessageProcessor.class );

        Set<URI> servers = new HashSet<URI>();
        servers.add(new URI("localhost:80"));
        servers.add(new URI("localhost:81"));
        servers.add(new URI("localhost:82"));
        when( context.getClusterNodeCount() ).thenReturn( 3 );
        when( context.getAckCount( 1 ) ).thenReturn( 2 );

        State state = new State( new EpochNumber() );
        when( context.getMyState() ).thenReturn( state );

        OmegaState.omega.handle( context, message, outgoing );

        verify( context ).roundDone( 1 );
        assertEquals( 1, state.getFreshness() );
    }

    @Test
    public void testRoundTripTimeoutAkaAdvanceEpoch() throws Throwable
    {
        OmegaContext context = mock( OmegaContext.class );
        Message<OmegaMessage> message = Message.internal( OmegaMessage.round_trip_timeout );
        MessageProcessor outgoing = mock( MessageProcessor.class );

        State state = new State( new EpochNumber() );
        when( context.getMyState() ).thenReturn( state );

        View myView = new View( state );
        when( context.getMyView() ).thenReturn( myView );

        OmegaState result = (OmegaState) OmegaState.omega.handle( context, message, outgoing );

        assertEquals( OmegaState.omega, result );
        verify( context ).getMyState();
        verify( context ).getMyView();
        verify( context, never() ).roundDone( anyInt() );
        assertTrue( myView.isExpired() );

        // Most important things to test - no update on freshness and serial num incremented
        assertEquals( 1, state.getEpochNum().getSerialNum() );
        assertEquals( 0, state.getFreshness() );

     }

    private static final String fromString = "neo4j://from";

    private void testRefreshResponseOnState(boolean newer) throws Throwable
    {
        OmegaContext context = mock( OmegaContext.class );
        Message<OmegaMessage> message = mock( Message.class );
        MessageProcessor outgoing = mock( MessageProcessor.class );

        // Value here is not important, we override the compareTo() method anyway
        RefreshPayload payload = new RefreshPayload( 1,1,1,1 );

        when( message.getHeader( Message.FROM ) ).thenReturn( fromString );
        when( message.getPayload() ).thenReturn( payload );
        when( message.getMessageType()).thenReturn( OmegaMessage.refresh );

        URI fromURI = new URI( fromString );

        Map<URI, State> registry = mock( Map.class );
        State fromState = mock( State.class );
        when( registry.get( fromURI ) ).thenReturn( fromState );
        when( context.getRegistry() ).thenReturn( registry );
        if ( newer )
        {
            when( fromState.compareTo( any(State.class) )).thenReturn( -1 );
        }
        else
        {
            when( fromState.compareTo( any(State.class) )).thenReturn( 1 );
        }

        OmegaState.omega.handle( context, message, outgoing );

        verify( context, atLeastOnce() ).getRegistry();
        verify( registry ).get( fromURI );
        verify( fromState ).compareTo( isA( State.class ) ); // existing compared to the one from the message
        if ( newer )
        {
            verify( registry ).put( eq( fromURI ), isA( State.class ) );
        }
        else
        {
            verify( registry, never() ).put( eq( fromURI ), isA( State.class ) );
        }

        verify( outgoing ).process( argThat( MessageArgumentMatcher.matchOn( fromURI, null, OmegaMessage.refresh_ack ) ) );
    }

    @Test
    public void testRefreshResponseOnOlderState() throws Throwable
    {
        testRefreshResponseOnState( false );
    }

    @Test
    public void testRefreshResponseOnNewerState() throws Throwable
    {
        testRefreshResponseOnState( true );
    }
}
