package protocols.dht.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

public class GetHostReplyMessage extends ProtoMessage {

    public static final short MSG_ID = 104;

    public GetHostReplyMessage() {
        super(MSG_ID);
    }
}
