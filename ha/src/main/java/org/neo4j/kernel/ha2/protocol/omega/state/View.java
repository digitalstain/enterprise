package org.neo4j.kernel.ha2.protocol.omega.state;

public class View
{
    private final State state;
    private boolean expired = true;

    public View( State state )
    {
        this.state = state;
    }

    public State getState()
    {
        return state;
    }

    public boolean isExpired()
    {
        return expired;
    }

    public void setExpired( boolean expired )
    {
        this.expired = expired;
    }
}
