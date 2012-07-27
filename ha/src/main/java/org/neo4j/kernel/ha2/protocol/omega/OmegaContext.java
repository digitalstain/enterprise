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
package org.neo4j.kernel.ha2.protocol.omega;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.neo4j.kernel.ha2.protocol.cluster.ClusterContext;
import org.neo4j.kernel.ha2.protocol.omega.state.State;
import org.neo4j.kernel.ha2.protocol.omega.state.View;

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

    public View getMyView()
    {
        return views.get( clusterContext.getMe() );
    }

    public int startCollectionRound()
    {
        return 0;  //To change body of created methods use File | Settings | File Templates.
    }

    public Map<URI, View> getPreviousViewForCollectionRound( int newCollectionRound )
    {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    public int getStatusResponsesForRound( int newCollectionRound )
    {
        return 0;  //To change body of created methods use File | Settings | File Templates.
    }

    public Map<URI, View> getViews()
    {
        return views;
    }

    public void collectionRoundDone( int firstCollectionRound )
    {

    }

    public void responseReceivedForRound( int firstCollectionRound )
    {

    }

    private static final class RefreshRoundContext
    {
        int acksReceived;
    }

    private final SortedMap<Integer, RefreshRoundContext> refreshContexts = new TreeMap<Integer, RefreshRoundContext>();

    public int getAckCount( int forRound )
    {
        RefreshRoundContext context = refreshContexts.get( forRound );
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

    public ClusterContext getClusterContext()
    {
        return clusterContext;
    }

    public void addListener( OmegaListener  listener )
    {
        listeners.add( listener );
    }

    public void removeListener( OmegaListener  listener )
    {
        listeners.remove( listener );
    }

    public int getClusterNodeCount()
    {
        return getClusterContext().getConfiguration().getNodes().size();
    }

    public Map<URI, State> getRegistry()
    {
        return registry;
    }
}
