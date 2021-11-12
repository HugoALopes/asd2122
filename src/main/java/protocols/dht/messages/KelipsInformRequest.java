package protocols.dht.messages;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import protocols.storage.messages.SaveMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class KelipsInformRequest extends ProtoMessage{
    public final static short REQUEST_ID = 132;
	
	private UUID uid;
	
	
	public KelipsInformRequest() {
		super(REQUEST_ID);
		this.uid = UUID.randomUUID();
	}

    public UUID getUid(){
        return this.uid;
    }

	//TODO - check serializer
	public static ISerializer<KelipsInformRequest> serializer = new ISerializer<>() {
		@Override
		public void serialize(KelipsInformRequest msg, ByteBuf out) throws IOException {
			out.writeLong(msg.uid.getMostSignificantBits());
			out.writeLong(msg.uid.getLeastSignificantBits());
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
