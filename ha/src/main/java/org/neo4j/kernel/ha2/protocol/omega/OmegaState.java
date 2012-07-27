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

import java.net.URI;

import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.kernel.ha2.protocol.omega.payload.RefreshAckPayload;
import org.neo4j.kernel.ha2.protocol.omega.payload.RefreshPayload;
import org.neo4j.kernel.ha2.protocol.omega.state.State;


public enum OmegaState implements org.neo4j.kernel.ha2.statemachine.State<OmegaContext, OmegaMessage>
{
    start
            {
                @Override
                public OmegaState handle( OmegaContext context, Message<OmegaMessage> message,
                                          MessageProcessor outgoing ) throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case add_listener:
                            context.addListener( (OmegaListener) message.getPayload() );
                            break;
                        case remove_listener:
                            context.removeListener( (OmegaListener) message.getPayload() );
                            break;
                        case start:
                            context.startTimers();
                            return omega;

                    }
                    return this;
                }
            },
    omega
            {
                @Override
                public org.neo4j.kernel.ha2.statemachine.State<?, ?> handle( OmegaContext context, Message<OmegaMessage> message,
                                           MessageProcessor outgoing ) throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case refresh_timeout:
                        {
                            int refreshRoundId = context.startRefreshRound();
                            for ( URI server : context.getServers() )
                            {
                                outgoing.process( Message.to(  OmegaMessage.refresh, server,
                                         RefreshPayload.fromState( context.getMyState(),
                                                refreshRoundId ) ) );
                            }
                        }
                        break;
                        case refresh:
                        {
                            State state = RefreshPayload.toState(
                                    (RefreshPayload) message.getPayload() ).other();
                            URI from = new URI( message.getHeader( Message.FROM ) );
                            State currentForThisHost = context.getRegistry().get( from );
                            if ( currentForThisHost == null || currentForThisHost.compareTo( state ) < 0 )
                            {
                                context.getRegistry().put( from, state );
                            }
                            outgoing.process( Message.to(
                                    OmegaMessage.refresh_ack,
                                    from,
                                    RefreshAckPayload.forRefresh( (RefreshPayload) message
                                            .getPayload() ) ) );
                        }
                        break;
                        case refresh_ack:
                        {
                            RefreshAckPayload ack = (RefreshAckPayload) message.getPayload();
                            // TODO deal with duplicate/corrupted messages here perhaps?
                            int refreshRound = ack.round;
                            context.ackReceived( refreshRound );
                            if ( context.getAckCount( refreshRound ) > context.getClusterNodeCount() / 2 )
                            {
                                context.getMyState().increaseFreshness();
                                context.roundDone( refreshRound );
                            }
                        }
                        break;
                        case round_trip_timeout:
                        {
                            context.getMyView().setExpired( true );
                            context.getMyState().getEpochNum().increaseSerialNum();
                        }
                        break;
                        default:
                            throw new IllegalArgumentException( message.getMessageType() +" is unknown" );
                    }
                    return this;
                }
            };
}
