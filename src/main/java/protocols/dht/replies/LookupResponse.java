package protocols.dht.replies;

import protocols.storage.replies.StoreOKReply;
import pt.unl.fct.di.novasys.babel.generic.ProtoReply;
import pt.unl.fct.di.novasys.network.data.Host;

import java.math.BigInteger;
import java.util.List;
import java.util.UUID;

public class LookupResponse extends ProtoReply {

    final public static short REPLY_ID = 120;

    private BigInteger id;
    private List<Host> host;
    private UUID mid;

    public LookupResponse(UUID mid,BigInteger id, List<Host> host) {
        super(this.REPLY_ID);
        this.id=id;
        this.host=host;
    }

    public BigInteger getObjId() {
        return id;
    }

    public List<Host> getHost() {
        return host;
    }

    public UUID getMid() {
        return mid;
    }
}
