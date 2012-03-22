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

package org.neo4j.kernel.ha2;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.com2.message.Message;

/**
 * Network failure strategy where you can declare, as the system runs,
 * what failures exist in the system.
 */
public class ScriptableNetworkFailureStrategy
    implements NetworkFailureStrategy
{
    List<String> nodesDown = new ArrayList<String>(  );
    List<String[]> linksDown = new ArrayList<String[]>(  );
    
    public ScriptableNetworkFailureStrategy nodeIsDown(String id)
    {
        nodesDown.add( id );
        return this;
    }
    
    public ScriptableNetworkFailureStrategy nodeIsUp(String id)
    {
        nodesDown.remove( id );
        return this;
    }
    
    public ScriptableNetworkFailureStrategy linkIsDown(String node1, String node2)
    {
        linksDown.add( new String[]{node1, node2} );
        linksDown.add( new String[]{ node2, node1 } );
        return this;
    }
    
    public ScriptableNetworkFailureStrategy linkIsUp(String node1, String node2)
    {
        linksDown.remove( new String[]{ node1, node2 } );
        linksDown.remove( new String[]{ node2, node1 } );
        return this;
    }
    
    @Override
    public boolean isLost( Message<?> message, String serverIdTo )
    {
        return nodesDown.contains( serverIdTo ) || linksDown.contains( new String[]{message.getHeader( Message.FROM ),serverIdTo} );
    }
}
