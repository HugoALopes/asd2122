package protocols.dht.kademlia.replies;

import java.math.BigInteger;
import java.util.List;
import java.util.UUID;

import protocols.dht.kademlia.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

public class KademliaFindNodeReply extends ProtoMessage {
    public final static short REQUEST_ID = 1051; //TODO: mudar o valor da constante

    private List<Node> closest_nodes;
    private BigInteger idToFind;
    private Node sender;
    private UUID uid; 

    public KademliaFindNodeReply(List<Node> closest_nodes, BigInteger idToFind, Node sender) {
        super(REQUEST_ID);
        uid = UUID.randomUUID();
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
