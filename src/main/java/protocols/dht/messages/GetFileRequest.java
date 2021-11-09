package protocols.dht.messages;

import java.math.BigInteger;
import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

public class GetFileRequest extends ProtoMessage{
    public final static short REQUEST_ID = 1070;
	
	private UUID uid;
    private BigInteger objID;


    public GetFileRequest(BigInteger objID) {
        super(REQUEST_ID);
        this.uid = UUID.randomUUID();
        this.objID = objID;
        //TODO Auto-generated constructor stub
    }

    public BigInteger getObjID(){
        return this.objID;
    }

    public UUID getUid(){
        return this.uid;
    }
    
}
