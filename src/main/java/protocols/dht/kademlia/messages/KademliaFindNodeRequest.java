package protocols.dht.kademlia.messages;

import java.math.BigInteger;
import java.util.UUID;

import protocols.dht.kademlia.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

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
}

