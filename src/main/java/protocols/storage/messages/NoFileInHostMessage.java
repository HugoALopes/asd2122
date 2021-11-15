package protocols.storage.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.UUID;

public class NoFileInHostMessage extends ProtoMessage {

    public static final short MSG_ID = 235;

    public UUID mid; 


    public NoFileInHostMessage(UUID mid) {
        super(MSG_ID);
        this.mid = mid;
       
    }

    public UUID getUid() {
        return mid;
    }

    public static ISerializer<NoFileInHostMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(NoFileInHostMessage msg, ByteBuf out) throws IOException {
            try{
            out.writeLong(msg.mid.getMostSignificantBits());
            out.writeLong(msg.mid.getLeastSignificantBits());

            out.writeShort(msg.getId());
            }catch (Exception e){
                e.printStackTrace(System.out);
                throw e;
            }
        }

        @Override
        public NoFileInHostMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);

            return new NoFileInHostMessage(mid);
        }
    };
}
