package protocols.dht.kademlia.messages;

import java.math.BigInteger;
import java.util.List;
import java.util.UUID;

import protocols.dht.kademlia.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

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
}
