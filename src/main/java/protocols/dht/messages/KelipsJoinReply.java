package protocols.dht.messages;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class KelipsJoinReply extends ProtoMessage{
    public final static short REQUEST_ID = 1051;

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
}
