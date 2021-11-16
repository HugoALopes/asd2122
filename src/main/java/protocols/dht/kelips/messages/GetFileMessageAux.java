package protocols.dht.kelips.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class GetFileMessageAux extends ProtoMessage {
    public final static short MESSAGE_ID = 138;

    private UUID uid;
    private BigInteger objID;
    private Host requester;


    public GetFileMessageAux(UUID mid, BigInteger objID, Host requester) {
        super(MESSAGE_ID);
        this.uid = mid;
        this.objID = objID;
        this.requester = requester;
    }

    public BigInteger getObjID() {
        return this.objID;
    }

    public UUID getUid() {
        return this.uid;
    }

    public Host getRequester() {
        return requester;
    }

    public static ISerializer<GetFileMessageAux> serializer = new ISerializer<>() {
        @SuppressWarnings("DuplicatedCode")
        @Override
        public void serialize(GetFileMessageAux msg, ByteBuf out) throws IOException {
            try {
                byte[] objId = msg.getObjID().toByteArray();
                out.writeInt(objId.length);
                if (objId.length > 0) {
                    out.writeBytes(objId);
                }
                out.writeLong(msg.uid.getMostSignificantBits());
                out.writeLong(msg.uid.getLeastSignificantBits());
                Host.serializer.serialize(msg.requester, out);
            } catch (Exception e) {
                e.printStackTrace(System.out);
                throw e;
            }
        }

        @SuppressWarnings("DuplicatedCode")
        @Override
        public GetFileMessageAux deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            byte[] objIdArr = new byte[size];
            if (size > 0)
                in.readBytes(objIdArr);
            BigInteger objId = new BigInteger(objIdArr);
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host req = Host.serializer.deserialize(in);
            return new GetFileMessageAux(mid, objId, req);
        }
    };

}
