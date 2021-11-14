package protocols.dht.kademlia;

import java.io.IOException;
import java.math.BigInteger;

import io.netty.buffer.ByteBuf;
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

    public static ISerializer<Node> serializer = new ISerializer<>() {

        @Override
        public void serialize(Node t, ByteBuf out) throws IOException {
            Host.serializer.serialize(t.getHost(), out);
            
            byte[] objId = t.getNodeId().toByteArray();
            out.writeInt(objId.length);
            if (objId.length > 0) {
                out.writeBytes(objId);
            }
        }

        @Override
        public Node deserialize(ByteBuf in) throws IOException {
            Host host = Host.serializer.deserialize(in);
            int size = in.readInt();
            byte[] objIdArr = new byte[size];
            if (size > 0)
                in.readBytes(objIdArr);
            BigInteger nodeId = new BigInteger(objIdArr);

            return new Node(host, nodeId);
        }
    };

}
