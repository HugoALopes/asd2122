package protocols.dht.kademlia.messages;

import java.io.IOException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import protocols.dht.kademlia.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

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

            Node.serializer.serialize(msg.getSenderNode(), out);

            out.writeShort(msg.getId());
            
        }

        @Override
        public KademliaJoinRequest deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
     
            Node sender = Node.serializer.deserialize(in);

            return new KademliaJoinRequest(sender);
        }
    };
}

