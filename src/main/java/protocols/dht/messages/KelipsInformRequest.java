package protocols.dht.messages;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

public class KelipsInformRequest extends ProtoMessage{
    public final static short REQUEST_ID = 1060;
	
	private UUID uid;
	
	public KelipsInformRequest() {
		super(REQUEST_ID);
		this.uid = UUID.randomUUID();
	}

    public UUID getUid(){
        return this.uid;
    }  
}
