package protocols.dht.kademlia.requests;

import java.math.BigInteger;
import java.util.UUID;

import protocols.dht.kademlia.Node;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

public class KademliaFindNodeRequest extends ProtoMessage{
    public final static short REQUEST_ID = 1050; //TODO: mudar o valor da constante
   
    private UUID uid;
    private BigInteger idToFind;
    private Node sender;
	
	public KademliaFindNodeRequest(BigInteger nodeToFind, Node sender) {
		super(REQUEST_ID);
		this.uid = UUID.randomUUID();
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

