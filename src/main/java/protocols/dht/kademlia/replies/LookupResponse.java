package protocols.dht.kademlia.replies;

import protocols.dht.kademlia.Node;
import protocols.storage.replies.StoreOKReply;
import pt.unl.fct.di.novasys.babel.generic.ProtoReply;

import java.util.List;

public class LookupResponse extends ProtoReply {

    final public static short REPLY_ID = 102;

    private List<Node> kclosest;

    public LookupResponse(List<Node> kclosest) {
        super(StoreOKReply.REPLY_ID);
        this.kclosest = kclosest;
    }

    public List<Node> getObjId() {
        return kclosest;
    }
}
