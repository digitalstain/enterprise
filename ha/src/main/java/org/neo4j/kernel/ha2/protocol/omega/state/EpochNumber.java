package org.neo4j.kernel.ha2.protocol.omega.state;

public class EpochNumber implements Comparable<EpochNumber>
{
    private int serialNum;
    private final int processId;

    public EpochNumber( int serialNum, int processId )
    {
        this.serialNum = serialNum;
        this.processId = processId;
    }

    public EpochNumber( int processId )
    {
        this( 0, processId );
    }

    public EpochNumber()
    {
        this(-1);
    }

    @Override
    public int compareTo( EpochNumber o )
    {
        return serialNum == o.serialNum ? processId - o.processId : serialNum - o.serialNum;
    }

    public int getSerialNum()
    {
        return serialNum;
    }

    public int getProcessId()
    {
        return processId;
    }

    public int increaseSerialNum()
    {
        return serialNum++;
    }
}
