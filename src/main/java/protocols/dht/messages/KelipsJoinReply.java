package protocols.dht.messages;

import java.io.*;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.Serializer;

import java.math.BigInteger;
import java.util.*;

public class KelipsJoinReply extends ProtoMessage{
    public final static short REQUEST_ID = 131;

    private UUID uid;
    private Map<Integer, Set<Host>> contacts;
    private Host sender;
    private Set<Host> agView;
    private Map<BigInteger, Host> fileTuples;

    public KelipsJoinReply(Host sender, Map<Integer, Set<Host>> contacts, Map<BigInteger, Host> fileTuples,
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

    public Map<BigInteger, Host> getFileTuples(){
        return this.fileTuples;
    }

    public Set<Host> getAgView(){
        return this.agView;
    }

    public Map<Integer, Set<Host>> getContacts(){
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
            Host.serializer.serialize(msg.getSender(), out);
            InformationGossip auxMsg = new InformationGossip(msg.contacts, msg.fileTuples, msg.agView);
            Serializer.serialize(auxMsg);

        }

        @Override
        public KelipsJoinReply deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            InformationGossip auxMsg = Serializer.deserialize(in.readBytes(in.readableBytes()).array());

            return new KelipsJoinReply(sender, auxMsg.getContacts(), auxMsg.getFileTuples(), auxMsg.getAgView());
        }
    };
}
