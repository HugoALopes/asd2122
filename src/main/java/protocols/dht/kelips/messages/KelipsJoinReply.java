package protocols.dht.kelips.messages;

import java.io.*;

import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.Serializer;

import java.math.BigInteger;
import java.util.*;

public class KelipsJoinReply extends ProtoMessage {
    private static final Logger logger = LogManager.getLogger(KelipsJoinReply.class);
    public final static short MESSAGE_ID = 131;

    private UUID uid;
    private Map<Integer, Set<Host>> contacts;
    private Host sender;
    private Set<Host> agView;
    private Map<BigInteger, Host> fileTuples;

    public KelipsJoinReply(Host sender, Map<Integer, Set<Host>> contacts, Map<BigInteger, Host> fileTuples,
                           Set<Host> agView) {
        super(MESSAGE_ID);
        this.contacts = contacts;
        this.sender = sender;
        this.uid = UUID.randomUUID();
        this.agView = agView;
        this.fileTuples = fileTuples;
    }

    public Host getSender() {
        return this.sender;
    }

    public Map<BigInteger, Host> getFileTuples() {
        return this.fileTuples;
    }

    public Set<Host> getAgView() {
        return this.agView;
    }

    public Map<Integer, Set<Host>> getContacts() {
        return this.contacts;
    }

    public UUID getUid() {
        return this.uid;
    }

    //TODO - check serializer
    public static ISerializer<KelipsJoinReply> serializer = new ISerializer<>() {
        @Override
        public void serialize(KelipsJoinReply msg, ByteBuf out) throws IOException {
            out.writeLong(msg.uid.getMostSignificantBits());
            out.writeLong(msg.uid.getLeastSignificantBits());
            Host.serializer.serialize(msg.getSender(), out);

            logger.info("kelips Join Reply Serializer before ser -> {}", msg.fileTuples);
            InformationGossip auxMsg = new InformationGossip(msg.contacts, msg.fileTuples, msg.agView);

            Serializer.serialize(auxMsg, out);
        }

        @Override
        public KelipsJoinReply deserialize(ByteBuf in) throws IOException {
            logger.debug("kelips Join Reply deSerializer begin");
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);

            InformationGossip auxMsg = Serializer.deserialize(in);

            KelipsJoinReply k = new KelipsJoinReply(sender, auxMsg.getContacts(), auxMsg.getFileTuples(), auxMsg.getAgView());
            logger.debug("kelips Join Reply deSerializer {}", k.sender);
            return k;
        }
    };

}
