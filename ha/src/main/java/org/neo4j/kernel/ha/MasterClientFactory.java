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
import org.neo4j.com.StoreIdGetter;
import org.neo4j.kernel.impl.util.StringLogger;

public interface MasterClientFactory
{
    public static final MasterClientFactory DEFAULT_FACTORY = null;

    public MasterClient instantiate( String hostNameOrIp, int port, StringLogger stringLogger,
            StoreIdGetter storeIdGetter, int readTimeoutSeconds, int lockReadTimeout, int maxConcurrentChannels );

    public static final MasterClientFactory F153 = new MasterClientFactory()
    {
        public MasterClient instantiate( String hostNameOrIp, int port, StringLogger stringLogger,
                StoreIdGetter storeIdGetter, int readTimeoutSeconds, int lockReadTimeout, int maxConcurrentChannels )
        {
            return new MasterClient153( hostNameOrIp, port, stringLogger, storeIdGetter,
                    ConnectionLostHandler.NO_ACTION, readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels );
        }
    };

    public static final MasterClientFactory F18 = new MasterClientFactory()
    {
        public MasterClient instantiate( String hostNameOrIp, int port, StringLogger stringLogger,
                StoreIdGetter storeIdGetter, int readTimeoutSeconds, int lockReadTimeout, int maxConcurrentChannels )
        {
            return new MasterClient18( hostNameOrIp, port, stringLogger, storeIdGetter,
                    ConnectionLostHandler.NO_ACTION, readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels );
        }
    };
}
