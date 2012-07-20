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

import java.util.List;

import javax.transaction.Transaction;

import org.neo4j.com.Response;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.ha.LockResult;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.impl.core.GraphProperties;
import org.neo4j.kernel.impl.transaction.IllegalResourceException;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockManagerImpl;
import org.neo4j.kernel.impl.transaction.LockNotFoundException;
import org.neo4j.kernel.impl.transaction.RagManager;
import org.neo4j.kernel.info.LockInfo;

public class HaLockManager implements LockManager
{
    private ComRequestSupport requestSupport;
    private final LockManagerImpl local;
    private Master master;
    private final TransactionSupport transactionSupport;

    public HaLockManager( ComRequestSupport requestSupport, TransactionSupport transactionSupport,
            RagManager ragManager )
    {
        this.requestSupport = requestSupport;
        this.transactionSupport = transactionSupport;
        this.local = new LockManagerImpl( ragManager );
    }
    
    @Override
    public long getDetectedDeadlockCount()
    {
        return local.getDetectedDeadlockCount();
    }

    @Override
    public void getReadLock( Object resource ) throws DeadlockDetectedException, IllegalResourceException
    {
        if ( getReadLockOnMaster( resource ) )
        {
            local.getReadLock( resource );
        }
    }

    @Override
    public void getReadLock( Object resource, Transaction tx ) throws DeadlockDetectedException, IllegalResourceException
    {
        local.getReadLock( resource, tx );
    }

    void masterChanged( Master master )
    {
        this.master = master;
    }
    
    private boolean getReadLockOnMaster( Object resource )
    {
        Response<LockResult> response = null;
        if ( resource instanceof Node )
        {
            transactionSupport.makeSureTxHasBeenInitialized();
            response = master.acquireNodeReadLock( requestSupport.getRequestContext(), ((Node)resource).getId() );
        }
        else if ( resource instanceof Relationship )
        {
            transactionSupport.makeSureTxHasBeenInitialized();
            response = master.acquireRelationshipReadLock( requestSupport.getRequestContext(), ((Relationship)resource).getId() );
        }
        else if ( resource instanceof GraphProperties )
        {
            transactionSupport.makeSureTxHasBeenInitialized();
            response = master.acquireGraphReadLock( requestSupport.getRequestContext() );
        }
        else
        {
            return true;
        }
        return receiveLockResponse( response );
    }

    private boolean receiveLockResponse( Response<LockResult> response )
    {
        requestSupport.receive( response );
        LockResult result = response.response();
        switch ( result.getStatus() )
        {
        case DEAD_LOCKED:
            throw new DeadlockDetectedException( result.getDeadlockMessage() );
        case NOT_LOCKED:
            throw new UnsupportedOperationException();
        case OK_LOCKED:
            break;
        default:
            throw new UnsupportedOperationException( result.toString() );
        }
        
        return true;
    }

    @Override
    public void getWriteLock( Object resource ) throws DeadlockDetectedException, IllegalResourceException
    {
        if ( getWriteLockOnMaster( resource ) )
        {
            local.getWriteLock( resource );
        }
    }

    @Override
    public void getWriteLock( Object resource, Transaction tx ) throws DeadlockDetectedException, IllegalResourceException
    {
        local.getWriteLock( resource, tx );
    }

    private boolean getWriteLockOnMaster( Object resource )
    {
        Response<LockResult> response = null;
        if ( resource instanceof Node )
        {
            transactionSupport.makeSureTxHasBeenInitialized();
            response = master.acquireNodeWriteLock( requestSupport.getRequestContext(), ((Node)resource).getId() );
        }
        else if ( resource instanceof Relationship )
        {
            transactionSupport.makeSureTxHasBeenInitialized();
            response = master.acquireRelationshipWriteLock( requestSupport.getRequestContext(), ((Relationship)resource).getId() );
        }
        else if ( resource instanceof GraphProperties )
        {
            transactionSupport.makeSureTxHasBeenInitialized();
            response = master.acquireGraphWriteLock( requestSupport.getRequestContext() );
        }
        else
        {
            return true;
        }
        
        return receiveLockResponse( response );
    }
    
    @Override
    public void releaseReadLock( Object resource, Transaction tx ) throws LockNotFoundException,
            IllegalResourceException
    {
        local.releaseReadLock( resource, tx );
    }

    @Override
    public void releaseWriteLock( Object resource, Transaction tx ) throws LockNotFoundException,
            IllegalResourceException
    {
        local.releaseWriteLock( resource, tx );
    }

    @Override
    public void dumpLocksOnResource( Object resource )
    {
        local.dumpLocksOnResource( resource );
    }

    @Override
    public List<LockInfo> getAllLocks()
    {
        return local.getAllLocks();
    }

    @Override
    public List<LockInfo> getAwaitedLocks( long minWaitTime )
    {
        return local.getAwaitedLocks( minWaitTime );
    }

    @Override
    public void dumpRagStack()
    {
        local.dumpRagStack();
    }

    @Override
    public void dumpAllLocks()
    {
        local.dumpAllLocks();
    }
}
