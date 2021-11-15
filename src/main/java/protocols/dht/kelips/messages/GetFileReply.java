package protocols.dht.kelips.messages;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.Serializer;

public class GetFileReply extends ProtoMessage {
    public final static short MESSAGE_ID = 133;

    private UUID uid;
    private BigInteger objID;
    private Host host;


    public GetFileReply(BigInteger objID, UUID uid, Host host) {
        super(MESSAGE_ID);
        this.objID = objID;
        this.uid = uid;
        this.host=host;
    }

    public BigInteger getObjID() {
        return this.objID;
    }

    public UUID getUid() {
        return this.uid;
    }

    public Host getHost() {
        return host;
    }

    public static ISerializer<GetFileReply> serializer = new ISerializer<>() {
        @SuppressWarnings("DuplicatedCode")
        @Override
        public void serialize(GetFileReply msg, ByteBuf out) throws IOException {
            try{
            byte[] objId = msg.getObjID().toByteArray();
            out.writeInt(objId.length);
            if (objId.length > 0) {
                out.writeBytes(objId);
            }
            out.writeLong(msg.uid.getMostSignificantBits());
            out.writeLong(msg.uid.getLeastSignificantBits());
            Host.serializer.serialize(msg.getHost(), out);
            }catch (Exception e){
                e.printStackTrace(System.out);
                throw e;
            }
        }

        @SuppressWarnings("DuplicatedCode")
        @Override
        public GetFileReply deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            byte[] objIdArr = new byte[size];
            if (size > 0)
                in.readBytes(objIdArr);
            BigInteger objId = new BigInteger(objIdArr);
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            return new GetFileReply(objId, mid, sender);
        }
    };
}
