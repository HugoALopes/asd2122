package protocols.dht.messages;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

public class KelipsInformReply extends ProtoMessage{
    public final static short REQUEST_ID = 1061;

    private UUID uid;

    public KelipsInformReply() {
        super(REQUEST_ID);
        this.uid = UUID.randomUUID();
    }

    public UUID getUid(){
        return this.uid;
    } 
    
}
