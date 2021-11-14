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

public class KademliaFindNodeRequest extends ProtoMessage{
    public final static short MESSAGE_ID = 1131;
   
    private UUID uid;
    private BigInteger idToFind;
    private Node sender;
    private Node dest;
	
	public KademliaFindNodeRequest(UUID mid, BigInteger nodeToFind, Node sender, Node dest) {
		super(MESSAGE_ID);
		this.uid = mid;
        this.idToFind = nodeToFind;
        this.sender = sender;
        this.dest = dest;
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

    public Node getDest(){
        return dest;
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

            out.writeBytes(message.getSender().getHost().getAddress().getAddress());
            out.writeShort(message.getSender().getHost().getPort());
            byte[] nodeId = message.getSender().getNodeId().toByteArray();
            out.writeInt(nodeId.length);
            if (nodeId.length > 0) {
                out.writeBytes(nodeId);
            }

            out.writeBytes(message.getDest().getHost().getAddress().getAddress());
            out.writeShort(message.getDest().getHost().getPort());
            nodeId = message.getDest().getNodeId().toByteArray();
            out.writeInt(nodeId.length);
            if (nodeId.length > 0) {
                out.writeBytes(nodeId);
            }

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
            
            byte[] addrBytes = new byte[4];
            in.readBytes(addrBytes);
            int port = in.readShort() & '\uffff';
            size = in.readInt();
            byte[] nodeId = new byte[size];
            if (size > 0)
                in.readBytes(nodeId);
            BigInteger nId = new BigInteger(nodeId);
            Node sender = new Node(new Host(InetAddress.getByAddress(addrBytes), port), nId);
            
            addrBytes = new byte[4];
            in.readBytes(addrBytes);
            port = in.readShort() & '\uffff';
            size = in.readInt();
            nodeId = new byte[size];
            if (size > 0)
                in.readBytes(nodeId);
            nId = new BigInteger(nodeId);
            Node dest = new Node(new Host(InetAddress.getByAddress(addrBytes), port), nId);

            return new KademliaFindNodeRequest(mid, nodeToFind, sender, dest);
        }
    };
}

