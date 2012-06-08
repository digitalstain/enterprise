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

package org.neo4j.kernel.ha2.protocol.atomicbroadcast;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * AtomicBroadcast payload. Wraps a byte buffer and its length.
 */
public class Payload
    implements Externalizable
{
    byte[] buf;
    int len;

    public Payload()
    {
    }

    public Payload( byte[] buf, int len )
    {
        this.buf = buf;
        this.len = len;
    }

    public byte[] getBuf()
    {
        return buf;
    }

    public int getLen()
    {
        return len;
    }

    @Override
    public void writeExternal( ObjectOutput out )
        throws IOException
    {
        out.write( len );
        out.write( buf, 0, len );
//        System.out.println( "Wrote payload" );
    }

    @Override
    public void readExternal( ObjectInput in )
        throws IOException, ClassNotFoundException
    {
        try
        {
            len = in.read(  );
            buf = new byte[len];
            in.read(buf, 0, len);
        }
        catch( Throwable e )
        {
            e.printStackTrace();
        }
//        System.out.println( "Read payload" );
    }
}
