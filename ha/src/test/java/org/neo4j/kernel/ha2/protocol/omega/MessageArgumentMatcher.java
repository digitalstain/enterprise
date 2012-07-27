package org.neo4j.kernel.ha2.protocol.omega;

import java.net.URI;

import org.hamcrest.Description;
import org.mockito.ArgumentMatcher;
import org.neo4j.com_2.message.Message;
import org.neo4j.com_2.message.MessageType;

public class MessageArgumentMatcher<T extends MessageType> extends ArgumentMatcher<Message<T>>
{
    private final URI from;
    private final URI to;
    private final T theMessageType;

    public MessageArgumentMatcher( URI from, URI to, T theMessageType )
    {
        this.from = from;
        this.to = to;
        this.theMessageType = theMessageType;
    }

    public static <T extends MessageType> MessageArgumentMatcher<T> matchOn(URI from, URI to, T theMessageType )
    {
        return new MessageArgumentMatcher( from, to, theMessageType );
    }

    @Override
    public boolean matches( Object message )
    {
        if (message == null || !(message instanceof Message))
        {
            return false;
        }
        if (to == null)
        {
            return true;
        }
        Message toMatchAgainst = (Message) message;
        boolean toMatches = to == null ? true : to.toString().equals( toMatchAgainst.getHeader( Message.TO ) );
        boolean fromMatches = from == null ? true : from.toString().equals( toMatchAgainst.getHeader( Message.FROM ) );
        boolean typeMatches = theMessageType == null ? true : theMessageType == toMatchAgainst.getMessageType();
        return fromMatches && toMatches && typeMatches;
    }

    @Override
    public void describeTo( Description description )
    {
        description.appendText( theMessageType.name()+"{from="+from+", to="+to+"}" );
    }
}
