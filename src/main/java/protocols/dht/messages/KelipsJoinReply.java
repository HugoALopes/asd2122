package protocols.dht.messages;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class KelipsJoinReply extends ProtoMessage{
    public final static short REQUEST_ID = 131;

    private UUID uid;
    private Map<Integer, ArrayList<Host>> contacts;
    private Host sender;
    private Set<Host> agView;
    private Map<Integer, Host> fileTuples;

    public KelipsJoinReply(Map<Integer, ArrayList<Host>> contacts, Host sender, Map<Integer, Host> fileTuples, 
        Set<Host> agView) {
        super(REQUEST_ID);
        this.contacts = contacts;
        this.sender = sender;
        this.uid = UUID.randomUUID();
        this.agView = agView;
        this.fileTuples = fileTuples;
    }
    
    public Host getSender(){
        return this.sender;
    }

    public Map<Integer, Host> getFileTuples(){
        return this.fileTuples;
    }

    public Set<Host> getAgView(){
        return this.agView;
    }

    public Map<Integer, ArrayList<Host>> getContacts(){
        return this.contacts;
    }
    
    public UUID getUid(){
        return this.uid;
    }

    //TODO - check serializer
    public static ISerializer<KelipsJoinReply> serializer = new ISerializer<>() {
        @Override
        public void serialize(KelipsJoinReply msg, ByteBuf out) throws IOException {
            out.writeLong(msg.uid.getMostSignificantBits());
            out.writeLong(msg.uid.getLeastSignificantBits());
            ObjectOutputStream oos = new ObjectOutputStream();
            oos.writeObject(msg.contacts);
            Host.serializer.serialize(msg.getSender(), out);
            out.writeShort(msg.getId());
            byte[] objId = msg.getObjId().toByteArray();
            out.writeInt(objId.length);
            if (objId.length > 0) {
                out.writeBytes(objId);
            }
            out.writeInt(msg.content.length);
            if (msg.content.length > 0) {
                out.writeBytes(msg.content);
            }
        }

        @Override
        public KelipsJoinReply deserialize(ByteBuf in) throws IOException {
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
            return new KelipsJoinReply(mid, objId, sender, content);
        }
    };
}
