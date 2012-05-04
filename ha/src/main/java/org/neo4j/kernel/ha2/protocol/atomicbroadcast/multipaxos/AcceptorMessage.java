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

import java.io.Serializable;
import org.neo4j.com2.message.MessageType;

/**
 * Acceptor state machine messages
 */
public enum AcceptorMessage
    implements MessageType
{
    failure,
    join,leave,
    prepare, // phase 1a/1b
    accept( ProposerMessage.phase2Timeout, LearnerMessage.learn); // phase 2a/2b - timeout if resulting learn is not fast enough

    private MessageType failureMessage;
    private MessageType[] next;

    private AcceptorMessage()
    {
        next = new AcceptorMessage[0];
    }

    private AcceptorMessage( MessageType failureMessage, MessageType... next )
    {
        this.failureMessage = failureMessage;
        this.next = next;
    }

    @Override
    public MessageType[] next()
    {
        return next;
    }

    @Override
    public MessageType failureMessage()
    {
        return failureMessage;
    }

    public static class PrepareState
        implements Serializable
    {
        private final long instanceId;
        private final long ballot;

        public PrepareState( long instanceId, long ballot )
        {
            this.instanceId = instanceId;
            this.ballot = ballot;
        }

        public long getInstanceId()
        {
            return instanceId;
        }

        public long getBallot()
        {
            return ballot;
        }
    }

    public static class AcceptState
        implements Serializable
    {
        private long instance;
        private long ballot;
        private Object value;

        public AcceptState( long instance, long ballot, Object value )
        {
            this.instance = instance;
            this.ballot = ballot;
            this.value = value;
        }

        public long getInstance()
        {
            return instance;
        }

        public long getBallot()
        {
            return ballot;
        }

        public Object getValue()
        {
            return value;
        }
    }

    public static class AcceptedState
        implements Serializable
    {
        private long v_rnd;
        private int c_vid;

        public AcceptedState( long v_rnd, int c_vid )
        {
            this.v_rnd = v_rnd;
            this.c_vid = c_vid;
        }

        public long getV_rnd()
        {
            return v_rnd;
        }

        public int getC_vid()
        {
            return c_vid;
        }
    }
}
