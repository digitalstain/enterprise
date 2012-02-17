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
package org.neo4j.kernel.ha;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.xaframework.RecoveryVerifier;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInfo;

public class SlaveRecoveryVerifier implements RecoveryVerifier
{
    private final Broker broker;
    private final StoreId storeId;

    public SlaveRecoveryVerifier( Broker broker, StoreId storeId )
    {
        this.broker = broker;
        this.storeId = storeId;
    }

    @Override
    public boolean isValid( TransactionInfo txInfo )
    {
        if ( txInfo.isOnePhase() ) return true;
        
        Pair<Master, Machine> master = broker.getMaster();
        Pair<Integer, Long> checksum = master.first().getMasterIdForCommittedTx( txInfo.getTxId(), storeId ).response();
        return checksum.first().intValue() == txInfo.getMaster() && checksum.other() == txInfo.getChecksum();
    }
}
