package org.neo4j.kernel.ha2.protocol.omega.state;

public class State implements Comparable<State>
{
    private final EpochNumber epochNum;
    private int freshness;

    public State( EpochNumber epochNum, int freshness )
    {
        this.epochNum = epochNum;
        this.freshness = freshness;
    }

    public State( EpochNumber epochNum )
    {
        this(epochNum, 0);
    }

    @Override
    public int compareTo( State o )
    {
        return epochNum.compareTo( o.epochNum ) == 0 ? freshness - o.freshness : epochNum.compareTo( o.epochNum );
    }

    public EpochNumber getEpochNum()
    {
        return epochNum;
    }

    public int getFreshness()
    {
        return freshness;
    }

    public int increaseFreshness()
    {
        return freshness++;
    }
}
