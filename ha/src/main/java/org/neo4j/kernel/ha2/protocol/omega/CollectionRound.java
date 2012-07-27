package org.neo4j.kernel.ha2.protocol.omega;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.ha2.protocol.omega.state.View;

public class CollectionRound
{
    private final Map<URI, View> oldViews;
    private final Map<URI, View> incomingViews;
    private final Set<URI> responders;

    private final int collectionRound;

    public CollectionRound( Map<URI, View> oldViews, int collectionRound )
    {
        this.oldViews = oldViews;
        this.incomingViews = new HashMap<URI, View>();
        this.collectionRound = collectionRound;
        this.responders = new HashSet<URI>();
    }


}
