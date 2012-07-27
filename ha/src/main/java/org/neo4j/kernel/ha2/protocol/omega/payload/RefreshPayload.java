package org.neo4j.kernel.ha2.protocol.omega.payload;

import java.io.Serializable;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha2.protocol.omega.state.EpochNumber;
import org.neo4j.kernel.ha2.protocol.omega.state.State;

public final class RefreshPayload implements Serializable
{
    public final int serialNum;
    public final int processId;
    public final int freshness;
    public final int refreshRound;

    public RefreshPayload( int serialNum, int processId, int freshness, int refreshRound )
    {
        this.serialNum = serialNum;
        this.processId = processId;
        this.freshness = freshness;
        this.refreshRound = refreshRound;
    }

    public static RefreshPayload fromState(State state, int refreshRound )
    {
        return new RefreshPayload( state.getEpochNum().getSerialNum(), state.getEpochNum().getProcessId(), state.getFreshness(), refreshRound );
    }

    public static Pair<Integer, State> toState( RefreshPayload payload )
    {
        EpochNumber epochNumber = new EpochNumber(payload.serialNum, payload.processId);
        State result = new State(epochNumber, payload.freshness);
        return Pair.of(payload.refreshRound, result);
    }
}
