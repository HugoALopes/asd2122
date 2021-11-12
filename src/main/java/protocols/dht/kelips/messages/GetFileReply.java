package protocols.dht.kelips.messages;

import java.math.BigInteger;
import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class GetFileReply extends ProtoMessage{
    public final static short MESSAGE_ID = 133;
	
	private UUID uid;
    private BigInteger objID;
    private Host host;


    public GetFileReply(BigInteger objID, UUID uid, Host host) {
        super(MESSAGE_ID);
        this.objID = objID;
        this.uid = uid;
    }

    public BigInteger getObjID(){
        return this.objID;
    }
    
    public UUID getUid(){
        return this.uid;
    }

    public Host getHost() {
        return host;
    }
}
