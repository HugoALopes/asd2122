package protocols.dht.messages;

import java.math.BigInteger;
import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

public class GetFileReply extends ProtoMessage{
    public final static short REQUEST_ID = 1071;
	
	private UUID uid;
    private BigInteger objID;


    public GetFileReply(BigInteger objID, UUID uid) {
        super(REQUEST_ID);
        this.objID = objID;
        this.uid = uid;
    }

    public BigInteger getObjID(){
        return this.objID;
    }
    
    public UUID getUid(){
        return this.uid;
    }
}
