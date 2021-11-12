package protocols.dht.kademlia.messages;

import java.util.UUID;

import protocols.dht.kademlia.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

//TODO
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
}

