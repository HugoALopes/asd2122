package protocols.dht.kademlia.messages;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import pt.unl.fct.di.novasys.network.data.Host;
import io.netty.buffer.ByteBuf;
import protocols.dht.kademlia.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class KademliaFindNodeReply extends ProtoMessage {
    public final static short MESSAGE_ID = 1132;

    private List<Node> closest_nodes;
    private BigInteger idToFind;
    private Node sender;
    private UUID uid; 
    private Host dest;

    public KademliaFindNodeReply(UUID mid, List<Node> closest_nodes, BigInteger idToFind, Node sender, Host dest) {
        super(MESSAGE_ID);
        uid = mid;
        this.closest_nodes = closest_nodes;
        this.idToFind = idToFind;
        this.sender = sender;
        this.dest = dest;
    }

    public UUID getUid(){
        return this.uid;
    }

    public List<Node> getClosestNodes() {
        return closest_nodes;
    }

    public BigInteger getIdToFind(){
        return idToFind;
    }

    public Node getSender(){
        return sender;
    }
    
    public Host getDest(){
    	return dest;
    }


    public static ISerializer<KademliaFindNodeReply> serializer = new ISerializer<>() {
        @Override
        public void serialize(KademliaFindNodeReply msg, ByteBuf out) throws IOException {
            out.writeLong(msg.getUid().getMostSignificantBits());
            out.writeLong(msg.getUid().getLeastSignificantBits());
            out.writeBytes(msg.getSender().getHost().getAddress().getAddress());
            out.writeShort(msg.getSender().getHost().getPort());
            byte[] nodeId = msg.getSender().getNodeId().toByteArray();
            out.writeInt(nodeId.length);
            if (nodeId.length > 0) {
                out.writeBytes(nodeId);
            }

            byte[] objId = msg.getIdToFind().toByteArray();
            out.writeInt(objId.length);
            if (objId.length > 0) {
                out.writeBytes(objId);
            }

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(msg.getClosestNodes());
            byte[] bytes = bos.toByteArray();
            out.writeInt(bytes.length);
            if (bytes.length > 0) {
                out.writeBytes(bytes);
            }

            out.writeBytes(msg.getDest().getAddress().getAddress());
            out.writeShort(msg.getDest().getPort());
       
        }


        @Override
        public KademliaFindNodeReply deserialize(ByteBuf in) throws IOException {
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
            Node node = new Node(new Host(InetAddress.getByAddress(addrBytes), port), nId);
            
            size = in.readInt();
            byte[] objIdArr = new byte[size];
            if (size > 0)
                in.readBytes(objIdArr);
            BigInteger nodeToFind = new BigInteger(objIdArr);

            size = in.readInt();
            byte[] arr = new byte[size];
            in.duplicate().readBytes(arr);
            List<Node> list = null;

            try {
                list = (ArrayList<Node>) new ObjectInputStream(new ByteArrayInputStream(arr)).readObject();
            } catch (Exception e) {
                e.printStackTrace();
            }

            addrBytes = new byte[4];
            in.readBytes(addrBytes);
            port = in.readShort() & '\uffff';
            Host dest = new Host(InetAddress.getByAddress(addrBytes), port);

            return new KademliaFindNodeReply(mid, list, nodeToFind, node, dest);
        }
    };

}
