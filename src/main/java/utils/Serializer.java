package utils;

import java.io.*;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.kelips.messages.InformationGossip;
import pt.unl.fct.di.novasys.network.data.Host;


@SuppressWarnings("All")
public class Serializer {
    private static final Logger logger = LogManager.getLogger(Serializer.class);

    public static byte[] serialize(InformationGossip obj) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(obj);
            out.flush();
        } catch (Exception e) {
        }
        return bos.toByteArray();
    }

    public static void serialize(InformationGossip msg, ByteBuf out) {
        serializeMapI_SHosts(msg.getContacts(), out);
        serializeMapBI_H(msg.getFileTuples(), out);
        serializeSetHosts(msg.getAgView(), out);
    }

    private static void serializeMapI_SHosts(Map<Integer, Set<Host>> obj, ByteBuf outBuff) {
        outBuff.writeInt(obj.size());
        obj.forEach((i, s) -> {
            outBuff.writeInt(i);
            serializeSetHosts(s, outBuff);
        });
    }

    private static void serializeMapBI_H(Map<BigInteger, Host> obj, ByteBuf outBuff) {
        outBuff.writeInt(obj.size());
        obj.forEach((b, h) -> {
            byte[] arr = b.toByteArray();
            outBuff.writeInt(arr.length);
            if (arr.length > 0) {
                outBuff.writeBytes(arr);
            }
            try {
                Host.serializer.serialize(h, outBuff);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private static void serializeSetHosts(Set<Host> obj, ByteBuf outBuff) {
        outBuff.writeInt(obj.size());
        obj.forEach(h -> {
            try {
                Host.serializer.serialize(h, outBuff);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    /*
    public static InformationGossip deserialize(byte[] bytes) {
        logger.info("inside Deserializer");
        InformationGossip o = null;
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        //ObjectInput in = null;
        try {
            logger.info("inside Deserializer try");
            ObjectInput in = new ObjectInputStream(bis);
            logger.info("inside Deserializer try 2 -> {}", in.readObject());
            o = (InformationGossip) in.readObject();
            logger.info("inside Deserializer obj -> {}", o.getContacts());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return o;
    }*/

    public static InformationGossip deserialize(ByteBuf in) {

        int mapISH_Size = in.readInt();
        Map<Integer, Set<Host>> contacts = new HashMap<>();
        for (int i = 0; i < mapISH_Size; i++) {
            int key = in.readInt();
            int auxSetSize = in.readInt();
            Set<Host> value = new HashSet<>();
            for (int j = 0; j < auxSetSize; j++) {
                try {
                    Host h = Host.serializer.deserialize(in);
                    value.add(h);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            contacts.put(key, value);
        }

        int mapBIH_Size = in.readInt();
        Map<BigInteger, Host> filetuples = new HashMap<>();
        for (int i = 0; i < mapBIH_Size; i++) {
            try {
                int size = in.readInt();
                byte[] objIdArr = new byte[size];
                if (size > 0)
                    in.readBytes(objIdArr);
                BigInteger key = new BigInteger(objIdArr);
                Host value = Host.serializer.deserialize(in);
                filetuples.put(key, value);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        int setH_Size = in.readInt();
        Set<Host> agView = new HashSet<>();
        for (int i = 0; i < setH_Size; i++) {
            try {
                Host h = Host.serializer.deserialize(in);
                agView.add(h);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return new InformationGossip(contacts, filetuples, agView);
    }
}
