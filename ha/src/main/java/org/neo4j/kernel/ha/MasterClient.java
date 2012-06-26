package org.neo4j.kernel.ha;

import static org.neo4j.com.Protocol18.readString;
import static org.neo4j.com.Protocol18.writeString;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.neo4j.com.Deserializer;
import org.neo4j.com.ObjectSerializer;
import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.StoreWriter;
import org.neo4j.com.TxExtractor;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.StoreId;

public interface MasterClient extends Master
{
    static final ObjectSerializer<LockResult> LOCK_SERIALIZER = new ObjectSerializer<LockResult>()
    {
        public void write( LockResult responseObject, ChannelBuffer result ) throws IOException
        {
            result.writeByte( responseObject.getStatus().ordinal() );
            if ( responseObject.getStatus().hasMessage() )
            {
                writeString( result, responseObject.getDeadlockMessage() );
            }
        }
    };

    static final Deserializer<LockResult> LOCK_RESULT_DESERIALIZER = new Deserializer<LockResult>()
    {
        public LockResult read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
        {
            LockStatus status = LockStatus.values()[buffer.readByte()];
            return status.hasMessage() ? new LockResult( readString( buffer ) ) : new LockResult( status );
        }
    };

    public Response<IdAllocation> allocateIds( final IdType idType );

    public Response<Integer> createRelationshipType( SlaveContext context, final String name );

    public Response<Void> initializeTx( SlaveContext context );

    public Response<LockResult> acquireNodeWriteLock( SlaveContext context, long... nodes );

    public Response<LockResult> acquireNodeReadLock( SlaveContext context, long... nodes );

    public Response<LockResult> acquireRelationshipWriteLock( SlaveContext context, long... relationships );

    public Response<LockResult> acquireRelationshipReadLock( SlaveContext context, long... relationships );

    public Response<LockResult> acquireGraphWriteLock( SlaveContext context );

    public Response<LockResult> acquireGraphReadLock( SlaveContext context );

    public Response<LockResult> acquireIndexReadLock( SlaveContext context, String index, String key );

    public Response<LockResult> acquireIndexWriteLock( SlaveContext context, String index, String key );

    public Response<Long> commitSingleResourceTransaction( SlaveContext context, final String resource,
            final TxExtractor txGetter );

    public Response<Void> finishTransaction( SlaveContext context, final boolean success );

    public void rollbackOngoingTransactions( SlaveContext context );

    public Response<Void> pullUpdates( SlaveContext context );

    public Response<Pair<Integer, Long>> getMasterIdForCommittedTx( final long txId, StoreId storeId );

    public Response<Void> copyStore( SlaveContext context, final StoreWriter writer );

    public Response<Void> copyTransactions( SlaveContext context, final String ds, final long startTxId,
            final long endTxId );
}
