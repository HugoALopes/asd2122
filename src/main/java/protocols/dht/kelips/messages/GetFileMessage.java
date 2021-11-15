package protocols.dht.kelips.messages;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class GetFileMessage extends ProtoMessage {
    public final static short MESSAGE_ID = 134;

    private UUID uid;
    private BigInteger objID;


    public GetFileMessage(UUID mid, BigInteger objID) {
        super(MESSAGE_ID);
        this.uid = mid;
        this.objID = objID;
    }

    public BigInteger getObjID() {
        return this.objID;
    }

    public UUID getUid() {
        return this.uid;
    }

    public static ISerializer<GetFileMessage> serializer = new ISerializer<>() {
        @SuppressWarnings("DuplicatedCode")
        @Override
        public void serialize(GetFileMessage msg, ByteBuf out) throws IOException {
            try{
            byte[] objId = msg.getObjID().toByteArray();
            out.writeInt(objId.length);
            if (objId.length > 0) {
                out.writeBytes(objId);
            }
            out.writeLong(msg.uid.getMostSignificantBits());
            out.writeLong(msg.uid.getLeastSignificantBits());
            }catch (Exception e){
                e.printStackTrace(System.out);
                throw e;
            }
        }

        @SuppressWarnings("DuplicatedCode")
        @Override
        public GetFileMessage deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            byte[] objIdArr = new byte[size];
            if (size > 0)
                in.readBytes(objIdArr);
            BigInteger objId = new BigInteger(objIdArr);
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            return new GetFileMessage(mid, objId);
        }
    };

}
