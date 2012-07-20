package org.neo4j.kernel.ha2.protocol.omega;

import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.neo4j.com_2.message.MessageType;

public enum OmegaMessage implements MessageType
{
    /*
     * Boring administrative messages
     */
    add_listener, remove_listener, start,
    /*
     * The required timeouts
     */
    refreshTimeout, roundTripTimeout, readTimeout,
    /*
     * The refresh request, where we send our state to
     * f other processes
     */
    refresh,
    /*
     * The message to respond with on refresh requests
     */
    refresh_ack,
    /*
     * The collect request, sent to gather up the states of
     * n-f other machines
     */
    collect,
    /*
     * The response to a collect request, sending
     * back the registry
     */
    status;

    public static final class RefreshPayload implements Serializable
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

        public static RefreshPayload fromState(OmegaContext.State state, int refreshRound )
        {
            return new RefreshPayload( state.epochNum.serialNum, state.epochNum.processId, state.freshness, refreshRound );
        }

        public static int toState( RefreshPayload payload, OmegaContext.State state )
        {
            state.epochNum.serialNum = payload.serialNum;
            state.epochNum.processId = payload.processId;
            state.freshness = payload.freshness;
            return payload.refreshRound;
        }
    }

    public static final class CollectPayload implements Serializable
    {
        public final URI[] servers;
        public final RefreshPayload[] registry;

        public CollectPayload( URI[] servers, RefreshPayload[] registry )
        {
            this.servers = servers;
            this.registry = registry;
        }

        public static CollectPayload fromRegistry( Map<URI, OmegaContext.State> registry )
        {
            URI[] servers = new URI[registry.size()];
            RefreshPayload[] refreshPayloads = new RefreshPayload[registry.size()];
            int currentIndex = 0;
            for (Map.Entry<URI, OmegaContext.State> entry : registry.entrySet())
            {
                servers[currentIndex] = entry.getKey();
                refreshPayloads[currentIndex] = RefreshPayload.fromState( entry.getValue(), -1 );
            }
            return new CollectPayload( servers, refreshPayloads );
        }
    }
}
