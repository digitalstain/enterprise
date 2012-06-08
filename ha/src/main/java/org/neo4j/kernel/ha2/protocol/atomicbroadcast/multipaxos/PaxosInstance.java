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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Record of an individual Paxos instance
 */
public class PaxosInstance
{
    enum State
    {
        empty,
        p1_pending,
        p1_ready,
        p2_pending,
        closed,
        delivered
    }

    PaxosInstanceStore store;
    InstanceId id = null;
    State state = State.empty;
    long ballot = 0;
    List<URI> acceptors;
    List<ProposerMessage.PromiseState> promises = new ArrayList<ProposerMessage.PromiseState>();
    List<ProposerMessage.AcceptedState> accepts = new ArrayList<ProposerMessage.AcceptedState>();
    Object value_1;
    long phase1Ballot = 0;
    Object value_2;
    boolean clientValue = false;

    public PaxosInstance( PaxosInstanceStore store )
    {
        this.store = store;
    }

    public boolean isState( State s )
    {
        return state.equals( s );
    }

    public void propose( InstanceId instanceId, long ballot, List<URI> acceptors)
    {
        this.state = State.p1_pending;
        this.id = instanceId;
        this.acceptors = acceptors;
        this.ballot = ballot;
    }
    public void phase1Timeout( long ballot)
    {
        this.ballot = ballot;
        promises.clear();
    }

    public void prepare( InstanceId instanceId, long ballot )
    {
        this.state = State.p1_pending;
        this.id = instanceId;
        this.ballot = ballot;
    }

    public void promise( ProposerMessage.PromiseState promiseState )
    {
        promises.add( promiseState );
        if (promiseState.getValue() != null && promiseState.getBallot() > phase1Ballot)
        {
            value_1 = promiseState.getValue();
            phase1Ballot = promiseState.getBallot();
        }
    }

    public void ready( Object value, boolean clientValue )
    {
        state = State.p1_ready;
        promises.clear();
        value_1 = null;
        phase1Ballot = 0;
        value_2 = value;
        this.clientValue = clientValue;
    }

    public void pending()
    {
        state = State.p2_pending;
    }

    public void acceptRejected()
    {
        state = State.empty;
        promises.clear();
        acceptors = null;
        phase1Ballot = 0;
        value_1 = null;
        value_2 = null;
        clientValue = false;
    }

    public void phase2Timeout( long ballot )
    {
        state = State.p1_pending;
        this.ballot = ballot;
        promises.clear();
        value_1 = null;
        phase1Ballot = 0;
    }

    public void accept( Object value )
    {
        state = State.p2_pending;
        value_2 = value;
    }

    public void accepted( ProposerMessage.AcceptedState acceptedState )
    {
        accepts.add( acceptedState );
    }

    public void closed(Object value)
    {
        value_2 = value;
        state = State.closed;
        accepts.clear();
        acceptors = null;
    }

    public void delivered()
    {
        state = State.delivered;
        store.delivered( id );
    }

    public List<URI> getAcceptors()
    {
        return acceptors;
    }

    @Override
    public String toString()
    {
        return id+": "+state.name()+" b="+ballot;
    }
}
