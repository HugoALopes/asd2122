package protocols.dht.kelips.messages;

import java.math.BigInteger;
import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

public class GetFileMessage extends ProtoMessage{
    public final static short MESSAGE_ID = 134;
	
	private UUID uid;
    private BigInteger objID;


    public GetFileMessage(UUID mid, BigInteger objID) {
        super(MESSAGE_ID);
        this.uid = mid;
        this.objID = objID;
    }

    public BigInteger getObjID(){
        return this.objID;
    }

    public UUID getUid(){
        return this.uid;
    }
    
}
