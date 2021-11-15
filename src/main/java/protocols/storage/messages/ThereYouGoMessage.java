package protocols.storage.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class ThereYouGoMessage extends ProtoMessage {

    public static final short MSG_ID = 234;

    private final UUID mid;
    private BigInteger id;
    private Host host;
    private byte[] content;

    @Override
    public String toString() {
        return "ThereYouGoMessage{" +
                "mid=" + mid +
                '}';
    }

    public ThereYouGoMessage(UUID mid, BigInteger id, Host host, byte[] content) {
        super(MSG_ID);
        this.mid=mid;
        this.id = id;
        this.host=host;
        this.content = content;
    }

    public BigInteger getObjId() {
        return id;
    }

    public UUID getMid() {
        return mid;
    }

    public Host getHost() {
        return host;
    }

    public byte[] getContent() {
        return content;
    }

    //TODO - check serializer
    public static ISerializer<ThereYouGoMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ThereYouGoMessage ThereYouGoMessage, ByteBuf out) throws IOException {
            try{
            out.writeLong(ThereYouGoMessage.mid.getMostSignificantBits());
            out.writeLong(ThereYouGoMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(ThereYouGoMessage.getHost(), out);
            out.writeShort(ThereYouGoMessage.getId());
            byte[] objId = ThereYouGoMessage.getObjId().toByteArray();
            out.writeInt(objId.length);
            if (objId.length > 0) {
                out.writeBytes(objId);
            }
            out.writeInt(ThereYouGoMessage.content.length);
            if (ThereYouGoMessage.content.length > 0) {
                out.writeBytes(ThereYouGoMessage.content);
            }
            }catch (Exception e){
                e.printStackTrace(System.out);
                throw e;
            }
        }

        @Override
        public ThereYouGoMessage deserialize(ByteBuf in) throws IOException {
            try{
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();
            int size = in.readInt();
                       
            byte[] objIdArr = new byte[size];
            if (size > 0) //TODO - devia ser while?
                in.readBytes(objIdArr);
            BigInteger objId = new BigInteger(objIdArr);
            size = in.readInt();
            byte[] content = new byte[size];
            if (size > 0)
                in.readBytes(content);

            //TODO - Check
            return new ThereYouGoMessage(mid, objId, sender, content);
              }catch (Exception e){

                e.printStackTrace(System.out);
                throw e;
            }
        }
    };
}
