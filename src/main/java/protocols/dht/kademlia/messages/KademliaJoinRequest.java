package protocols.dht.kademlia.messages;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import protocols.dht.kademlia.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class KademliaJoinRequest extends ProtoMessage{
    public final static short MESSAGE_ID = 1130;
   
    private UUID uid;
    private Node senderNode;
	
	public KademliaJoinRequest(Node senderNode) {
		super(MESSAGE_ID);
		this.uid = UUID.randomUUID();
        this.senderNode = senderNode;
	}

    public UUID getUid(){
        return uid;
    }

    public Node getSenderNode(){
        return senderNode;
    }

    public static ISerializer<KademliaJoinRequest> serializer = new ISerializer<>() {
      
        @Override
        public void serialize(KademliaJoinRequest msg, ByteBuf out) throws IOException {
            out.writeLong(msg.uid.getMostSignificantBits());
            out.writeLong(msg.uid.getLeastSignificantBits());

            out.writeBytes(msg.getSenderNode().getHost().getAddress().getAddress());
            out.writeShort(msg.getSenderNode().getHost().getPort());
            byte[] nodeId = msg.getSenderNode().getNodeId().toByteArray();
            out.writeInt(nodeId.length);
            if (nodeId.length > 0) {
                out.writeBytes(nodeId);
            }

            out.writeShort(msg.getId());
            
        }

        @Override
        public KademliaJoinRequest deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
     
            byte[] addrBytes = new byte[4];
            in.readBytes(addrBytes);
            int port = in.readShort() & '\uffff';
            int size = in.readInt();
            byte[] nodeId = new byte[size];
            if (size > 0)
                in.readBytes(nodeId);
            BigInteger nId = new BigInteger(nodeId);
            Node sender = new Node(new Host(InetAddress.getByAddress(addrBytes), port), nId);

            return new KademliaJoinRequest(sender);
        }
    };
}

