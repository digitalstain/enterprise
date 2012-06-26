package org.neo4j.kernel.ha;

import org.neo4j.com.StoreIdGetter;
import org.neo4j.kernel.impl.util.StringLogger;

public interface MasterClientFactory
{
    public static final MasterClientFactory DEFAULT_FACTORY = null;

    public MasterClient instantiate( String hostNameOrIp, int port, StringLogger stringLogger,
            StoreIdGetter storeIdGetter, int readTimeoutSeconds, int lockReadTimeout, int maxConcurrentChannels );
}
