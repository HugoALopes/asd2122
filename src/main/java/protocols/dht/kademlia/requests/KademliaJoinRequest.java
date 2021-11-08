package protocols.dht.kademlia.requests;

import java.util.UUID;

import protocols.dht.kademlia.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

//TODO
public class KademliaJoinRequest extends ProtoMessage{
    public final static short REQUEST_ID = 1050; //TODO: mudar o valor da constante
   
    private UUID uid;
    private Node senderNode;
	
	public KademliaJoinRequest(Node senderNode) {
		super(REQUEST_ID);
		this.uid = UUID.randomUUID();
        this.senderNode = senderNode;
	}

    public UUID getUid(){
        return uid;
    }

    public Node getSenderNode(){
        return senderNode;
    }
}

