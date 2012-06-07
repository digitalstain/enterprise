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

package org.neo4j.kernel.ha2.protocol.heartbeat;

import java.net.URI;
import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.kernel.ha2.protocol.election.ElectionMessage;
import org.neo4j.kernel.ha2.statemachine.State;
import org.slf4j.LoggerFactory;

import static org.neo4j.com_2.message.Message.*;

/**
 * TODO
 */
public enum HeartbeatState
    implements State<HeartbeatContext, HeartbeatMessage>
{
    start
    {
        @Override
        public HeartbeatState handle( HeartbeatContext context,
                                             Message<HeartbeatMessage> message,
                                             MessageProcessor outgoing
        )
            throws Throwable
        {
            switch( message.getMessageType() )
            {
                case addHeartbeatListener:
                {
                    context.addHeartbeatListener((HeartbeatListener) message.getPayload());
                    break;
                }

                case removeHeartbeatListener:
                {
                    context.removeHeartbeatListener( (HeartbeatListener) message.getPayload());
                    break;
                }

                case join:
                {
                    // Setup heartbeat timeouts
                    context.startHeartbeatTimers(message);
                    return heartbeat;
                }
            }

            return this;
        }
    },

    heartbeat
    {
        @Override
        public HeartbeatState handle( HeartbeatContext context,
                                             Message<HeartbeatMessage> message,
                                             MessageProcessor outgoing
        )
            throws Throwable
        {
            switch( message.getMessageType() )
            {
                case i_am_alive:
                {
                    HeartbeatMessage.IAmAliveState state = (HeartbeatMessage.IAmAliveState) message.getPayload();

                    context.alive( state.getServer() );

                    context.getClusterContext().timeouts.cancelTimeout( HeartbeatMessage.i_am_alive+"-"+state.getServer() );
                    context.getClusterContext().timeouts.setTimeout( HeartbeatMessage.i_am_alive+"-"+state.getServer(), timeout( HeartbeatMessage.timed_out, message, state
                        .getServer() ) );

                    break;
                }

                case timed_out:
                {
                    URI server = message.getPayload();

                    // Check if this node is no longer a part of the cluster
                    if (context.getClusterContext().getConfiguration().getNodes().contains( server ))
                    {
                        context.suspect( server );

                        context.getClusterContext().timeouts.setTimeout( HeartbeatMessage.i_am_alive+"-"+server, timeout( HeartbeatMessage.timed_out, message, server ) );

                        // Send suspicions messages to all non-failed servers
                        for( URI aliveServer : context.getAlive() )
                        {
                            if (!aliveServer.equals( context.getClusterContext().getMe() ))
                                outgoing.process( Message.to( HeartbeatMessage.suspicions, aliveServer, new HeartbeatMessage.SuspicionsState(context.getSuspicionsFor( context.getClusterContext().getMe() )) ) );
                        }

                    } else
                    {
                        // If no longer part of cluster, then don't bother
                        context.serverLeftCluster( server );
                    }
                    break;
                }

                case send_heartbeat:
                {
                    URI to = message.getPayload();

                    // Check if this node is no longer a part of the cluster
                    if (context.getClusterContext().getConfiguration().getNodes().contains( to ))
                    {
                        // Send heartbeat message to given server
                        outgoing.process( to( HeartbeatMessage.i_am_alive, to, new HeartbeatMessage.IAmAliveState( context.getClusterContext().getMe())));

                        // Set new timeout to send heartbeat to this host
                        context.getClusterContext().timeouts.setTimeout( HeartbeatMessage.send_heartbeat + "-" + to, timeout( HeartbeatMessage.send_heartbeat, message, to ) );
                    }
                    break;
                }

                case reset_send_heartbeat:
                {
                    URI to = message.getPayload();
                    String timeoutName = HeartbeatMessage.send_heartbeat + "-" + to;
                    context.getClusterContext().timeouts.cancelTimeout( timeoutName );
                    context.getClusterContext().timeouts.setTimeout( timeoutName, Message.timeout( HeartbeatMessage.send_heartbeat, message, to ) );
                    break;
                }

                case suspicions:
                {
                    HeartbeatMessage.SuspicionsState suspicions = message.getPayload();
                    URI from = new URI( message.getHeader( Message.FROM ) );

                    context.suspicions( from, suspicions.getSuspicions() );

                    if (context.shouldPromoteMeToCoordinator())
                    {
                        // TODO Master election
                        LoggerFactory.getLogger(getClass()).info( "Elect me to be master! "+context.getClusterContext().getMe() );
                    }

                    break;
                }

                case leave:
                {
                    context.stopHeartbeatTimers();

                    return start;
                }
            }

            return this;
        }
    }
}