package org.neo4j.kernel.ha2.protocol.omega.payload;

import java.io.Serializable;

public final class RefreshAckPayload implements Serializable
{
    public final int round;

    public RefreshAckPayload( int round )
    {
        this.round = round;
    }

    public static RefreshAckPayload forRefresh( RefreshPayload payload )
    {
        return new RefreshAckPayload( payload.refreshRound );
    }
}
