package protocols.dht.kademlia;

import java.math.BigInteger;

import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class Node {
    
    private final Host host;
    private final BigInteger nodeId;

    public Node(Host host, BigInteger nodeId){
        this.host = host;
      
        if(nodeId.signum() == -1)
        	this.nodeId = nodeId.multiply(BigInteger.valueOf(-1));
        else
        	this.nodeId = nodeId;
    }

    public Host getHost(){
        return host;
    }

    public BigInteger getNodeId(){
        return nodeId;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        
        Node obj_aux = (Node) obj;
        if(obj_aux == this) return true;
        if(obj_aux.getHost().equals(obj)) return true;
        return super.equals(obj);
    }

}
