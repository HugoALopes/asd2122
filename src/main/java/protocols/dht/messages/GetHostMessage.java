package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class GetHostMessage extends ProtoMessage {

    public static final short MSG_ID = 103;
    private UUID mid;
    private BigInteger objId;

        public GetHostMessage(UUID mid, BigInteger objId) {
        super(MSG_ID);
        this.mid = mid;
        this.objId = objId;
    }

    public UUID getMid() {
        return mid;
    }

    public BigInteger getObjId() {
        return objId;
    }

    //TODO - check serializer
    public static ISerializer<GetHostMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(GetHostMessage getHostMessage, ByteBuf out) throws IOException {
            out.writeLong(getHostMessage.mid.getMostSignificantBits());
            out.writeLong(getHostMessage.mid.getLeastSignificantBits());
            byte[] objId = getHostMessage.getObjId().toByteArray();
            out.writeInt(objId.length);
            if (objId.length > 0) {
                out.writeBytes(objId);
            }
        }

        @Override
        public GetHostMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            int size = in.readInt();
            byte[] objIdArr = new byte[size];
            if (size > 0)
                in.readBytes(objIdArr);
            BigInteger objId = new BigInteger(objIdArr);

            //TODO - Check
            return new GetHostMessage(mid, objId);
        }
    };
}
