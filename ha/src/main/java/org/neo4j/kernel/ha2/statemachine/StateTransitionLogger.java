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

package org.neo4j.kernel.ha2.statemachine;

import java.util.logging.Logger;
import org.neo4j.kernel.ha2.protocol.RingParticipant;

/**
 * TODO
 */
public class StateTransitionLogger
        implements StateTransitionListener
{
    private RingParticipant participant;
    private Logger logger;

    public StateTransitionLogger(RingParticipant participant, Logger logger )
    {
        this.participant = participant;
        this.logger = logger;
    }

    public void stateTransition(StateTransition transition)
    {
        if (transition.getMessage().getPayload() instanceof String)
            logger.warning(participant.toString()+": "+
                               transition.getOldState().toString()+"-["+transition.getMessage().getMessageType()+":"+transition.getMessage().getPayload()+"]->"+
                                                                                                                               transition.getNewState().toString());
        else
            logger.info(participant.toString()+": "+transition.getOldState().toString()+"-["+transition.getMessage().getMessageType()+"]->"+transition.getNewState().toString());
    }
}