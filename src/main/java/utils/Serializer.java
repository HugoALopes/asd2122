package utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import protocols.dht.messages.InformationGossip;

public class Serializer {

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

    public static InformationGossip deserialize(byte[] bytes) {
        InformationGossip o = null;
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            o = (InformationGossip) in.readObject();

        } catch (Exception e) {
        }
        return o;
    }

    
}
