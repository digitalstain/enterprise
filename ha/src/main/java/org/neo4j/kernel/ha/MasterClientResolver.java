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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.util.StringLogger;

public class MasterClientResolver
{
    private static final class ProtocolCombo
    {
        final int applicationProtocol;
        final int internalProtocol;

        ProtocolCombo( int applicationProtocol, int internalProtocol )
        {
            this.applicationProtocol = applicationProtocol;
            this.internalProtocol = internalProtocol;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj == null )
            {
                return false;
            }
            if ( obj.getClass() != ProtocolCombo.class )
            {
                return false;
            }
            ProtocolCombo other = (ProtocolCombo) obj;
            return other.applicationProtocol == applicationProtocol && other.internalProtocol == internalProtocol;
        }

        @Override
        public int hashCode()
        {
            return ( 31 * applicationProtocol ) | internalProtocol;
        }

        static final ProtocolCombo PC_153 = new ProtocolCombo( 2, 2 );
        static final ProtocolCombo PC_18 = new ProtocolCombo( 2, 3 );
    }

    private final Map<ProtocolCombo, MasterClientFactory> protocolToFactoryMapping;

    public MasterClientResolver( StringLogger messageLogger, int readTimeout, int lockReadTimeout, int channels )
    {
        protocolToFactoryMapping = new HashMap<ProtocolCombo, MasterClientFactory>();
        protocolToFactoryMapping.put( ProtocolCombo.PC_153, new MasterClientFactory.F153( messageLogger, readTimeout,
                lockReadTimeout, channels ) );
        protocolToFactoryMapping.put( ProtocolCombo.PC_18, new MasterClientFactory.F18( messageLogger, readTimeout,
                lockReadTimeout, channels ) );
    }

    public MasterClientFactory getFor( int applicationProtocol, int internalProtocol )
    {
        System.out.println( "getting master client factory for application " + applicationProtocol + ", internal "
                            + internalProtocol );
        return protocolToFactoryMapping.get( new ProtocolCombo( applicationProtocol, internalProtocol ) );
    }

    public MasterClientFactory getDefault()
    {
        return getFor( ProtocolCombo.PC_18.applicationProtocol, ProtocolCombo.PC_18.internalProtocol );
    }
}
