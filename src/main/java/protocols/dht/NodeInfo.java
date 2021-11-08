package protocols.dht;

import pt.unl.fct.di.novasys.network.data.Host;

public class NodeInfo {

    private Host host;
    private long rtt;
    private int heartbeatCount;

    public NodeInfo(Host host, long rtt){
        this.host = host;
        this.rtt = rtt;
        this.heartbeatCount = 0;
    }

    public NodeInfo(Host host){
        this.host = host;
        this.rtt = -1;
        this.heartbeatCount = 0;
    }

    public Host getHost(){
        return this.host;
    }

    public long getRtt(){
        return this.rtt;
    }

    public int getHeartbeatCount(){
        return this.heartbeatCount;
    }

    public void incHeartbeatCount(){
        this.heartbeatCount++;
    }

    public void setRtt(long rtt){
        this.rtt = rtt;
    }

    @Override
    public boolean equals(Object obj) {
        NodeInfo n = (NodeInfo) obj;
        if(obj == null)
            return false;
        if(obj == this)
            return true;
        if(n.getHost().equals(this.host))
            return true;
        return super.equals(obj);
    }

}
