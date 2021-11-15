package protocols.storage.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class GetMessage extends ProtoMessage {

    public static final short MSG_ID = 232;

    private BigInteger id;
    private UUID mid;

    public GetMessage(UUID uid, BigInteger id) {
        super(MSG_ID);
        this.mid = uid;
        this.id = id;
    }

    public BigInteger getObjId() {
        return id;
    }

    public UUID getMid() {
        return mid;
    }

    public static ISerializer<GetMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(GetMessage saveMessage, ByteBuf out) throws IOException {
            try{
            out.writeLong(saveMessage.mid.getMostSignificantBits());
            out.writeLong(saveMessage.mid.getLeastSignificantBits());
            byte[] objId = saveMessage.getObjId().toByteArray();
            out.writeInt(objId.length);
            if (objId.length > 0) {
                out.writeBytes(objId);
            }
            }catch (Exception e){
                e.printStackTrace(System.out);
                throw e;
            }
        }

        @Override
        public GetMessage deserialize(ByteBuf in) throws IOException {
            try{
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            int size = in.readInt();
            byte[] objIdArr = new byte[size];
            if (size > 0)
                in.readBytes(objIdArr);
            BigInteger objId = new BigInteger(objIdArr);

            return new GetMessage(mid, objId);
            }catch (Exception e){
                e.printStackTrace(System.out);
                throw e;
            }
        }
    };
}
