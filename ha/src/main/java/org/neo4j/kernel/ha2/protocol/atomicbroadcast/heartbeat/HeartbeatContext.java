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

package org.neo4j.kernel.ha2.protocol.atomicbroadcast.heartbeat;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha2.timeout.Timeouts;

/**
 * TODO
 */
public class HeartbeatContext
{
    Timeouts timeouts;

    public String me;
    List<String> servers = new ArrayList<String>(  );
    List<String> failed = new ArrayList<String>(  );
    Iterable<HeartbeatListener> listeners = Listeners.newListeners();

    public HeartbeatContext(Timeouts timeouts)
    {
        this.timeouts = timeouts;
    }

    public void setPossibleServers(String[] serverIds)
    {
        servers.clear();
        servers.addAll( Iterables.toList( Iterables.iterable( serverIds ) ));
        failed.clear();
        failed.addAll( Iterables.toList( Iterables.iterable( serverIds ) ));
    }

    public void alive( final String server )
    {
        if (failed.remove( server ))
            Listeners.notifyListeners( listeners, new Listeners.Notification<HeartbeatListener>()
            {
                @Override
                public void notify( HeartbeatListener listener )
                {
                    listener.alive( server );
                }
            } );
    }

    public void failed( final String server )
    {
        if (!failed.contains( server ))
        {
            failed.add( server );
            Listeners.notifyListeners( listeners, new Listeners.Notification<HeartbeatListener>()
            {
                @Override
                public void notify( HeartbeatListener listener )
                {
                    listener.failed( server );
                }
            } );
        }
    }

    public void setMe( String me )
    {
        this.me = me;
    }

    public void addHeartbeatListener( HeartbeatListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    public void removeHeartbeatListener(HeartbeatListener listener)
    {
        listeners = Listeners.removeListener( listener, listeners );
    }
}
