package protocols.storage.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class SuccessSaveMessage extends ProtoMessage {

    public static final short MSG_ID = 231;
    private String name;
    private UUID mid;

    public SuccessSaveMessage(UUID uid, String name) {
        super(MSG_ID);
        //this.name=name;
        this.mid =uid;
    }

    public SuccessSaveMessage(UUID uid){
        super(MSG_ID);
        this.mid =uid;
    }

    public String getName() {
        return name;
    }

    public UUID getUid() {
        return mid;
    }

    //TODO - check serializer
    public static ISerializer<SuccessSaveMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(SuccessSaveMessage msg, ByteBuf out) throws IOException {
            try{
            out.writeLong(msg.mid.getMostSignificantBits());
            out.writeLong(msg.mid.getLeastSignificantBits());
            //out.writeBytes(msg.name.getBytes(StandardCharsets.UTF_8));
            }catch (Exception e){
                e.printStackTrace(System.out);
                throw e;
            }
        }

        @Override
        public SuccessSaveMessage deserialize(ByteBuf in) throws IOException {
            try{
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            //String name = in.toString();

            //TODO - Check .toString
            return new SuccessSaveMessage(mid);
             }catch (Exception e){
                e.printStackTrace(System.out);
                throw e;
            }
        }
    };
}
