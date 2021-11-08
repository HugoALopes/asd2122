package protocols.storage.messages;

import io.netty.buffer.ByteBuf;
import protocols.storage.requests.StoreRequest;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class SaveMessage extends ProtoMessage {

    public static final short MSG_ID = 230;

    private final UUID mid;
    private BigInteger id;
    private Host host;
    private byte[] content;

    @Override
    public String toString() {
        return "SaveMessage{" +
                "mid=" + mid +
                '}';
    }

    public SaveMessage(UUID mid, BigInteger id, Host host, byte[] content) {
        super(MSG_ID);
        this.mid=mid;
        this.id = id;
        this.host=host;
        this.content = content;
    }

    public BigInteger getObjId() {
        return id;
    }

    public Host getHost() {
        return host;
    }

    public byte[] getContent() {
        return content;
    }

    //TODO - check serializer
    public static ISerializer<SaveMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(SaveMessage saveMessage, ByteBuf out) throws IOException {
            out.writeLong(saveMessage.mid.getMostSignificantBits());
            out.writeLong(saveMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(saveMessage.getHost(), out);
            out.writeShort(saveMessage.getId());
            byte[] objId = saveMessage.getObjId().toByteArray();
            out.writeInt(objId.length);
            if (objId.length > 0) {
                out.writeBytes(objId);
            }
            out.writeInt(saveMessage.content.length);
            if (saveMessage.content.length > 0) {
                out.writeBytes(saveMessage.content);
            }
        }

        @Override
        public SaveMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            //short toDeliver = in.readShort();
            int size = in.readInt();
            byte[] objIdArr = new byte[size];
            if (size > 0)
                in.readBytes(objIdArr);
            BigInteger objId = new BigInteger(objIdArr);
            size = in.readInt();
            byte[] content = new byte[size];
            if (size > 0)
                in.readBytes(content);

            //TODO - Check
            return new SaveMessage(mid, objId, sender, content);
        }
    };
}
