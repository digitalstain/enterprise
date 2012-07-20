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

import javax.transaction.Transaction;

import org.neo4j.com.Response;
import org.neo4j.com.RequestContext;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

/**
 * Supplies HA services (for example lock manager, tx hook, etc.) with information they need.
 * An exhaustive, unorganized list of what all HA services need altogether.
 */
public interface HaServiceSupplier
{
    Master getMaster();

    RequestContext getRequestContext( int eventIdentifier );

    RequestContext getRequestContext( XaDataSource dataSource );
    
    RequestContext getRequestContext();
    
    RequestContext getEmptyRequestContext();
    
    void receive( Response<?> response );

    boolean hasAnyLocks( Transaction tx );

    int getMasterServerId();
    
    void makeSureTxHasBeenInitialized();
    
    int getMasterIdForTx( long tx );
    
    Triplet<Long, Integer, Long> getLastTx();

    // ...
}