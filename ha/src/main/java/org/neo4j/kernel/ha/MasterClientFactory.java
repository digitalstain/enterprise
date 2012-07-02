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

import org.neo4j.com.ConnectionLostHandler;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;

public interface MasterClientFactory
{
    public static final MasterClientFactory DEFAULT_FACTORY = null;

    public static abstract class AbstractMasterClientFactory implements MasterClientFactory
    {
        protected final StringLogger stringLogger;
        protected final int readTimeoutSeconds;
        protected final int lockReadTimeout;
        protected final int maxConcurrentChannels;

        protected AbstractMasterClientFactory( StringLogger stringLogger, int readTimeoutSeconds,
                int lockReadTimeout, int maxConcurrentChannels )
        {
            this.stringLogger = stringLogger;
            this.readTimeoutSeconds = readTimeoutSeconds;
            this.lockReadTimeout = lockReadTimeout;
            this.maxConcurrentChannels = maxConcurrentChannels;
        }

    }

    public static final class F153 extends AbstractMasterClientFactory
    {
        public F153( StringLogger stringLogger, int readTimeoutSeconds, int lockReadTimeout,
                int maxConcurrentChannels )
        {
            super( stringLogger, readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels );
        }

        public MasterClient instantiate( String hostNameOrIp, int port, StoreId storeId )
        {
            return new MasterClient153( hostNameOrIp, port, stringLogger, storeId, ConnectionLostHandler.NO_ACTION,
                    readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels );
        }
    };

    public static final class F18 extends AbstractMasterClientFactory
    {
        public F18( StringLogger stringLogger, int readTimeoutSeconds, int lockReadTimeout,
                int maxConcurrentChannels )
        {
            super( stringLogger, readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels );
        }

        public MasterClient instantiate( String hostNameOrIp, int port, StoreId storeId )
        {
            return new MasterClient18( hostNameOrIp, port, stringLogger, storeId, ConnectionLostHandler.NO_ACTION,
                    readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels );
        }
    };

    public MasterClient instantiate( String hostNameOrIp, int port, StoreId storeId );
}
