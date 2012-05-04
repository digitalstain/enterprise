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

package org.neo4j.kernel.ha2.protocol.atomicbroadcast.multipaxos;

import org.neo4j.com2.message.Message;
import org.neo4j.com2.message.MessageProcessor;
import org.neo4j.kernel.ha2.statemachine.State;

/**
 * State machine for Paxos Acceptor
 */
public enum AcceptorState
    implements State<PaxosContext, AcceptorMessage, AcceptorState>
{
    start
        {
            @Override
            public AcceptorState handle( PaxosContext context,
                                      Message<AcceptorMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch( message.getMessageType() )
                {
                    case join:
                    {
                        // Initialize all learner instances
                        context.acceptorInstances.clear();
                        for (int i = 0; i < 100; i++)
                            context.acceptorInstances.add( new AcceptorInstance() );

                        return acceptor;
                    }
                }

                return this;
            }
        },

    acceptor
        {
            @Override
            public AcceptorState handle( PaxosContext context,
                                      Message<AcceptorMessage> message,
                                      MessageProcessor outgoing
            )
                throws Throwable
            {
                switch( message.getMessageType() )
                {
                    case prepare:
                    {
                        AcceptorMessage.PrepareState prepareState = (AcceptorMessage.PrepareState) message.getPayload();
                        AcceptorInstance instance = context.acceptorInstances.get( context.getAcceptorInstanceIndex( prepareState.getInstanceId() ) );
                        if ( instance.instanceId < prepareState.getInstanceId())
                        {
                            // Reset instance - this assumes we are not working on more than n instances at any given time, where n is size of list
                            instance.instanceId = prepareState.getInstanceId();
                            instance.ballot = 0;
                            instance.value = null;
                        } else if (instance.instanceId > prepareState.getInstanceId())
                        {
                            // This proposer is out of date - ignore him
                            break;
                        }

                        if ( prepareState.getBallot() > instance.ballot )
                        {
                            instance.ballot = prepareState.getBallot();

                            outgoing.process(Message.to( ProposerMessage.promise, message.getHeader( Message.FROM ), new ProposerMessage.PromiseState( instance.instanceId, instance.ballot, instance.value ) ));
                        } else
                        {
                            // Optimization - explicit reject
                            outgoing.process(Message.to( ProposerMessage.reject, message.getHeader( Message.FROM ), new ProposerMessage.DenialState( instance.instanceId )));
                        }
                        break;
                    }

                    case accept:
                    {
                        // Task 4
                        AcceptorMessage.AcceptState acceptState = ( AcceptorMessage.AcceptState) message.getPayload();
                        AcceptorInstance instance = context.acceptorInstances.get( context.getAcceptorInstanceIndex( acceptState.getInstance() ) );

                        if (acceptState.getBallot() == instance.ballot)
                        {
                            instance.value = acceptState.getValue();
                            outgoing.process( Message.to( ProposerMessage.accepted, message.getHeader( Message.FROM ), new ProposerMessage.AcceptedState(instance.instanceId) ) );
                        } else
                        {
                            // TODO How to deal with this? What does this case mean?
                        }
                        break;
                    }

                    case leave:
                    {
                        // TODO Do formal leave process
                        return start;
                    }
                }

                return this;
            }
        },
}
