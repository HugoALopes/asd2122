package protocols.dht.kademlia.messages;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import protocols.broadcast.messages.FloodMessage;
import protocols.dht.kademlia.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class KademliaFindNodeReply extends ProtoMessage {
    public final static short MESSAGE_ID = 1132;

    private List<Node> closest_nodes;
    private BigInteger idToFind;
    private Node sender;
    private UUID uid; 

    public KademliaFindNodeReply(UUID mid, List<Node> closest_nodes, BigInteger idToFind, Node sender) {
        super(MESSAGE_ID);
        uid = mid;
        this.closest_nodes = closest_nodes;
        this.idToFind = idToFind;
        this.sender = sender;
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


    public static ISerializer<KademliaFindNodeReply> serializer = new ISerializer<>() {
        @Override
        public void serialize(KademliaFindNodeReply floodMessage, ByteBuf out) throws IOException {
            out.writeLong(floodMessage.getUid().getMostSignificantBits());
            out.writeLong(floodMessage.getUid().getLeastSignificantBits());
            Node.serializer.serialize(floodMessage.getSender(), out);
            byte[] objId = floodMessage.getIdToFind().toByteArray();
            out.writeInt(objId.length);
            if (objId.length > 0) {
                out.writeBytes(objId);
            }

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(floodMessage.getClosestNodes());
            byte[] bytes = bos.toByteArray();
            out.writeInt(bytes.length);
            if (bytes.length > 0) {
                out.writeBytes(bytes);
            }
       
        }


        @Override
        public KademliaFindNodeReply deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Node node = Node.serializer.deserialize(in);
            
            int size = in.readInt();
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

            return new KademliaFindNodeReply(mid, list, nodeToFind, node);
        }
    };

}
