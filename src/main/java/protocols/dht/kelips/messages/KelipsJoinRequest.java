package protocols.dht.kelips.messages;

import java.io.IOException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

public class KelipsJoinRequest extends ProtoMessage{
    public final static short MESSAGE_ID = 130;
	
	private UUID uid;
    private Host sender;
    private long time;
	
	public KelipsJoinRequest(Host sender) {
		super(MESSAGE_ID);
		this.uid = UUID.randomUUID();
        this.sender = sender;
        this.time = System.currentTimeMillis();
	}

    public UUID getUid(){
        return this.uid;
    }

    public Host getHost(){
        return this.sender;
    }
    
    public long getTime(){
        return this.time;
    }

    //TODO - check serializer
    public static ISerializer<KelipsJoinRequest> serializer = new ISerializer<>() {
        @Override
        public void serialize(KelipsJoinRequest msg, ByteBuf out) throws IOException {
            out.writeLong(msg.uid.getMostSignificantBits());
            out.writeLong(msg.uid.getLeastSignificantBits());
            Host.serializer.serialize(msg.getHost(), out);
            out.writeLong(msg.time);
        }

        @Override
        public KelipsJoinRequest deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            long time = in.readLong();

            //TODO - Check
            return new KelipsJoinRequest(sender);
        }
    };

}
