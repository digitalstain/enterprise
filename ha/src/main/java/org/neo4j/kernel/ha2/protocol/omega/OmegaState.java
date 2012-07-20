package org.neo4j.kernel.ha2.protocol.omega;

import java.net.URI;

import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageProcessor;
import org.neo4j.kernel.ha2.statemachine.State;

public enum OmegaState implements State<OmegaContext, OmegaMessage>
{
    start
            {
                @Override
                public OmegaState handle( OmegaContext context, Message<OmegaMessage> message, MessageProcessor outgoing ) throws Throwable
                {
                    switch (message.getMessageType())
                    {
                        case add_listener:
                            context.addListener((OmegaListener) message.getPayload());
                            break;
                        case remove_listener:
                            context.removeListener((OmegaListener) message.getPayload());
                            break;
                        case start:
                            context.startTimers();
                            return omega;

                    }
                    return this;
                }
            },
    omega
            {
                @Override
                public State<?, ?> handle( OmegaContext context, Message<OmegaMessage> message, MessageProcessor outgoing ) throws Throwable
                {
                    switch(message.getMessageType())
                    {
                        case refreshTimeout:
                            int refreshRoundId = context.startRefreshRound();
                            for (URI server : context.getServers())
                            {
                                outgoing.process( Message.to(OmegaMessage.refresh, server, OmegaMessage.RefreshPayload.fromState( context.getMyState(), refreshRoundId )) );
                            }
                            break;
                        case refresh :
                            OmegaContext.State state = new OmegaContext.State();
                            int refreshRound = OmegaMessage.RefreshPayload.toState( (OmegaMessage.RefreshPayload) message.getPayload(), state );
                            URI from = new URI( message.getHeader( Message.FROM ) );
                    }
                    return this;
                }
            };
}
