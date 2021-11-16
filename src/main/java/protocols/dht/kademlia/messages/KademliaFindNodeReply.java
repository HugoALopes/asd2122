package protocols.dht.kademlia.messages;

import java.io.IOException;
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

    public UUID getUid() {
        return this.uid;
    }

    public List<Node> getClosestNodes() {
        return closest_nodes;
    }

    public BigInteger getIdToFind() {
        return idToFind;
    }

    public Node getSender() {
        return sender;
    }

    public Host getDest() {
        return dest;
    }

    public static ISerializer<KademliaFindNodeReply> serializer = new ISerializer<>() {
        @Override
        public void serialize(KademliaFindNodeReply msg, ByteBuf out) throws IOException {
            //uid
            if (msg.uid == null) {
                out.writeInt(-1);
            } else {
                out.writeInt(0);
                out.writeLong(msg.uid.getMostSignificantBits());
                out.writeLong(msg.uid.getLeastSignificantBits());
            }

            //sender
            out.writeBytes(msg.getSender().getHost().getAddress().getAddress());
            out.writeShort(msg.getSender().getHost().getPort());
            byte[] nodeId = msg.getSender().getNodeId().toByteArray();
            out.writeInt(nodeId.length);
            if (nodeId.length > 0) {
                out.writeBytes(nodeId);
            }

            //idToFind
            byte[] objId = msg.getIdToFind().toByteArray();
            out.writeInt(objId.length);
            if (objId.length > 0) {
                out.writeBytes(objId);
            }

            //closestNodes
            out.writeInt(msg.getClosestNodes().size());
            for (Node n : msg.getClosestNodes()) {
                nodeId = n.getNodeId().toByteArray();
                out.writeInt(nodeId.length);
                if (nodeId.length > 0) {
                    out.writeBytes(nodeId);
                }
            }

            //dest
            out.writeBytes(msg.getDest().getAddress().getAddress());
            out.writeShort(msg.getDest().getPort());

        }

        @Override
        public KademliaFindNodeReply deserialize(ByteBuf in) throws IOException {
            int hasmid = in.readInt();
            UUID mid = null;
            if (hasmid == 0) {
                long firstLong = in.readLong();
                long secondLong = in.readLong();
                mid = new UUID(firstLong, secondLong);
            }

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
            List<Node> closestNodes = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                int size_aux = in.readInt();
                nodeId = new byte[size_aux];
                if (size_aux > 0)
                    in.readBytes(nodeId);
                nId = new BigInteger(nodeId);
                Node aux = new Node(new Host(InetAddress.getByAddress(addrBytes), port), nId);
                closestNodes.add(aux);
            }

            addrBytes = new byte[4];
            in.readBytes(addrBytes);
            port = in.readShort() & '\uffff';
            Host dest = new Host(InetAddress.getByAddress(addrBytes), port);

            return new KademliaFindNodeReply(mid, closestNodes, nodeToFind, node, dest);
        }
    };

}
