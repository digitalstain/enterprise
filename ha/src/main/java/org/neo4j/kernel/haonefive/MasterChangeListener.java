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
package org.neo4j.kernel.haonefive;

import java.net.URL;

/**
 * The MasterChangeListener is listening for events from master election, for example some state of
 * some services in the database must be switched accordingly. It can also provide input to the
 * election algorithm when if asked for.
 */
public interface MasterChangeListener
{
    /**
     * Called when new master has been elected. The new master may not be available a.t.m.
     * A call to {@link #newMasterBecameAvailable(String)} will confirm that the master given in
     * the most recent {@link #newMasterElected(String, int)} call is up and running as master.
     * 
     * @param masterUrl the connection information to the master.
     * @param masterServerId the server id of the master (TODO remove)
     */
    void newMasterElected( URL masterUrl, int masterServerId /*Here because other legacy HA code relies on it*/ );
    
    void newMasterBecameAvailable( URL masterUrl );
}
