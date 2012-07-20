package org.neo4j.kernel.ha2.protocol.omega;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.neo4j.kernel.ha2.protocol.cluster.ClusterContext;

public class OmegaContext
{
    public void startTimers()
    {
    }

    public void roundDone( int refreshRound )
    {
        refreshContexts.remove( refreshRound );
    }

    public Iterable<? extends URI> getServers()
    {
        return clusterContext.configuration.getNodes();
    }

    public State getMyState()
    {
        return registry.get( clusterContext.getMe() );
    }

    private static final class RefreshRoundContext
    {
        int acksReceived;
    }

    private final SortedMap<Integer, RefreshRoundContext> refreshContexts = new TreeMap<Integer, RefreshRoundContext>();

    public int getAckCount( int i )
    {
        RefreshRoundContext context = refreshContexts.get( i );
        if (context == null)
        {
            return -1;
        }
        return context.acksReceived;
    }

    public int startRefreshRound()
    {
        int nextRound = refreshContexts.isEmpty() ? 1 : refreshContexts.lastKey() + 1;
        refreshContexts.put( nextRound, new RefreshRoundContext() );
        return nextRound;
    }

    public void ackReceived( int forRound )
    {
        refreshContexts.get( forRound ).acksReceived++;
    }

    static final class EpochNumber implements Comparable<EpochNumber>
    {
        int serialNum;
        int processId;

        @Override
        public int compareTo( EpochNumber o )
        {
            return serialNum == o.serialNum ? processId - o.processId : serialNum - o.serialNum;
        }
    }

    static final class State implements Comparable<State>
    {
        EpochNumber epochNum;
        int freshness;

        @Override
        public int compareTo( State o )
        {
            return epochNum.compareTo( o.epochNum ) == 0 ? freshness - o.freshness : epochNum.compareTo( o.epochNum );
        }
    }

    static final class View
    {
        State state;
        boolean expired = true;
    }

    private final Map<URI, State> registry = new HashMap<URI, State>();
    private final Map<URI, View> views = new HashMap<URI, View>();
    private final List<OmegaListener> listeners = new ArrayList<OmegaListener>();
    private int bigDelta;
    private int smallDelta;
    private ClusterContext clusterContext;

    public OmegaContext( ClusterContext clusterContext )
    {
       this.clusterContext = clusterContext;
    }

    public void addListener( OmegaListener  listener )
    {
        listeners.add( listener );
    }

    public void removeListener( OmegaListener  listener )
    {
        listeners.remove( listener );
    }
}
