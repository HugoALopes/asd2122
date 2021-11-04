package protocols.storage.messages;

import protocols.storage.requests.StoreRequest;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.math.BigInteger;

public class SaveRequestMsg extends ProtoMessage {

    final public static short REQUEST_ID = 206;

    private BigInteger id;
    private Host host;
    private byte[] content;


    public SaveRequestMsg(BigInteger id, Host host, byte[] content) {
        super(StoreRequest.REQUEST_ID);
        this.id = id;
        this.host=host;
        this.content = content;
    }

    public BigInteger getObjId() {
        return id;
    }

    public Host getHost() {
        return host;
    }

    public byte[] getContent() {
        return content;
    }
}
