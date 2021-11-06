package protocols.dht.replies;

import java.util.Set;
import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoReply;
import pt.unl.fct.di.novasys.network.data.Host;

public class KelipsReplyJoinRequest extends ProtoReply{
    public final static short REQUEST_ID = 1051;

    private UUID uid;
    private Set<Host> agView;
    private Host sender;

    public KelipsReplyJoinRequest(Set<Host> agView, Host sender) {
        super(REQUEST_ID);
        this.agView = agView;
        this.sender = sender;
        this.uid = UUID.randomUUID();
    }
    
    public Host getSender(){
        return this.sender;
    }

    public Set<Host> getAgView(){
        return this.agView;
    }

    public UUID getUid(){
        return this.uid;
    }
}
