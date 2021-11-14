package protocols.dht.kademlia.messages;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import protocols.dht.kademlia.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class KademliaFindNodeRequest extends ProtoMessage{
    public final static short MESSAGE_ID = 1131;
   
    private UUID uid;
    private BigInteger idToFind;
    private Node sender;
	
	public KademliaFindNodeRequest(UUID mid, BigInteger nodeToFind, Node sender) {
		super(MESSAGE_ID);
		this.uid = mid;
        this.idToFind = nodeToFind;
        this.sender = sender;
	}

    public UUID getUid(){
        return uid;
    }

    public BigInteger getIdToFind(){
        return idToFind;
    }

    public Node getSender(){
        return sender;
    }

    public static ISerializer<KademliaFindNodeRequest> serializer = new ISerializer<>() {
        @Override
        public void serialize(KademliaFindNodeRequest message, ByteBuf out) throws IOException {
            out.writeLong(message.uid.getMostSignificantBits());
            out.writeLong(message.uid.getLeastSignificantBits());
           
            byte[] objId = message.getIdToFind().toByteArray();
            out.writeInt(objId.length);
            if (objId.length > 0) {
                out.writeBytes(objId);
            }

            Node.serializer.serialize(message.getSender(), out);

            out.writeShort(message.getId());
        }

        @Override
        public KademliaFindNodeRequest deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            int size = in.readInt();
            byte[] objIdArr = new byte[size];
            if (size > 0)
                in.readBytes(objIdArr);
            BigInteger nodeToFind = new BigInteger(objIdArr);
            Node sender = Node.serializer.deserialize(in);

            return new KademliaFindNodeRequest(mid, nodeToFind, sender);
        }
    };
}

