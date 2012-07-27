package org.neo4j.kernel.ha2.protocol.omega.payload;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;

import org.neo4j.kernel.ha2.protocol.omega.state.State;

public final class CollectPayload implements Serializable
{
    public final URI[] servers;
    public final RefreshPayload[] registry;

    public CollectPayload( URI[] servers, RefreshPayload[] registry )
    {
        this.servers = servers;
        this.registry = registry;
    }

    public static CollectPayload fromRegistry( Map<URI, State> registry )
    {
        URI[] servers = new URI[registry.size()];
        RefreshPayload[] refreshPayloads = new RefreshPayload[registry.size()];
        int currentIndex = 0;
        for (Map.Entry<URI, State> entry : registry.entrySet())
        {
            servers[currentIndex] = entry.getKey();
            refreshPayloads[currentIndex] = RefreshPayload.fromState( entry.getValue(), -1 );
        }
        return new CollectPayload( servers, refreshPayloads );
    }
}