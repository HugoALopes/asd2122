package protocols.dht.kelips.messages;

import java.io.IOException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class KelipsInformRequest extends ProtoMessage{
    public final static short MESSAGE_ID = 132;
	
	private UUID uid;
	
	
	public KelipsInformRequest() {
		super(MESSAGE_ID);
		this.uid = UUID.randomUUID();
	}

    public UUID getUid(){
        return this.uid;
    }

	//TODO - check serializer
	public static ISerializer<KelipsInformRequest> serializer = new ISerializer<>() {
		@Override
		public void serialize(KelipsInformRequest msg, ByteBuf out) throws IOException {
			try{
			out.writeLong(msg.uid.getMostSignificantBits());
			out.writeLong(msg.uid.getLeastSignificantBits());
			}catch (Exception e){
				e.printStackTrace(System.out);
				throw e;
			}
		}

		@Override
		public KelipsInformRequest deserialize(ByteBuf in) throws IOException {
			long firstLong = in.readLong();
			long secondLong = in.readLong();
			UUID mid = new UUID(firstLong, secondLong);

			//TODO - Check
			return new KelipsInformRequest();
		}
	};
}
