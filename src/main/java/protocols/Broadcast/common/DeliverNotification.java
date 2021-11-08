package protocols.Broadcast.common;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class DeliverNotification extends ProtoNotification {
    public static final short NOTIFICATION_ID = 201;

    private final Host sender;
    private final Host aux;
    private final UUID msgId;
    private final byte[] msg;

    public DeliverNotification(UUID msgId, Host sender, byte[] msg, Host aux) {
        super(NOTIFICATION_ID);
        this.msgId = msgId;
        this.sender = sender;
        this.msg = msg;
        this.aux = aux;
    }

    public Host getSender() {
        return sender;
    }

    public Host getAux() {
        return aux;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public byte[] getMsg() {
        return msg;
    }
}
