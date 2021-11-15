package protocols.dht.kelips.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class GetDiffAgFileMessage extends ProtoMessage {
    public final static short MESSAGE_ID = 135;

    private UUID uid;
    private BigInteger objID;
    private boolean opType;


    public GetDiffAgFileMessage(UUID mid, BigInteger objID, boolean opType) {
        super(MESSAGE_ID);
        this.uid = mid;
        this.objID = objID;
        this.opType = opType;
    }

    public BigInteger getObjID() {
        return this.objID;
    }

    public UUID getUid() {
        return this.uid;
    }

    public boolean getOpType() {
        return opType;
    }

    public static ISerializer<GetDiffAgFileMessage> serializer = new ISerializer<>() {
        @SuppressWarnings("DuplicatedCode")
        @Override
        public void serialize(GetDiffAgFileMessage msg, ByteBuf out) throws IOException {
            try{
            byte[] objId = msg.getObjID().toByteArray();
            out.writeInt(objId.length);
            if (objId.length > 0) {
                out.writeBytes(objId);
            }
            out.writeLong(msg.uid.getMostSignificantBits());
            out.writeLong(msg.uid.getLeastSignificantBits());
            out.writeBoolean(msg.opType);
            }catch (Exception e){
                e.printStackTrace(System.out);
                throw e;
            }
        }

        @SuppressWarnings("DuplicatedCode")
        @Override
        public GetDiffAgFileMessage deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            byte[] objIdArr = new byte[size];
            if (size > 0)
                in.readBytes(objIdArr);
            BigInteger objId = new BigInteger(objIdArr);
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            boolean opType = in.readBoolean();
            return new GetDiffAgFileMessage(mid, objId, opType);
        }
    };
}
