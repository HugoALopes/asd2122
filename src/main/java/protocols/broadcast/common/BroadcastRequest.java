package protocols.broadcast.common;

import java.util.Set;
import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

public class BroadcastRequest extends ProtoRequest {

    public static final short REQUEST_ID = 410;

    private final Host sender;
    private final UUID msgId;
    private final byte[] msg;
    private final Set<Host> group;

    public BroadcastRequest(UUID msgId, Host sender, byte[] msg, Set<Host> group) {
        super(REQUEST_ID);
        this.msgId = msgId;
        this.sender = sender;
        this.msg = msg;
        this.group=group;
    }

    public Host getSender() {
        return sender;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public byte[] getMsg() {
        return msg;
    }

    public Set<Host> getGroup() {
        return group;
    }
}
