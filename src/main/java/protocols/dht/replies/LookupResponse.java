package protocols.dht.replies;

import protocols.storage.replies.StoreOKReply;
import pt.unl.fct.di.novasys.babel.generic.ProtoReply;
import pt.unl.fct.di.novasys.network.data.Host;

import java.math.BigInteger;

public class LookupResponse extends ProtoReply {

    final public static short REPLY_ID = 120;

    private BigInteger id;
    private Host host;

    public LookupResponse(BigInteger id, Host host) {
        super(StoreOKReply.REPLY_ID);
        this.id=id;
        this.host=host;
    }

    public BigInteger getObjId() {
        return id;
    }

    public Host getHost() {
        return host;
    }

}
