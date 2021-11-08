package protocols.dht.messages;

import java.util.UUID;

import protocols.dht.NodeInfo;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class KelipsJoinReply extends ProtoMessage{
    public final static short REQUEST_ID = 1051;

    private UUID uid;
    private Map<Integer, ArrayList<NodeInfo>> contacts;
    private Host sender;
    private Set<NodeInfo> agView;
    private Map<Integer, NodeInfo> fileTuples;

    public KelipsJoinReply(Map<Integer, ArrayList<NodeInfo>> contacts, Host sender, Map<Integer, NodeInfo> fileTuples, 
        Set<NodeInfo> agView) {
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

    public Map<Integer, NodeInfo> getFileTuples(){
        return this.fileTuples;
    }

    public Set<NodeInfo> getAgView(){
        return this.agView;
    }

    public Map<Integer, ArrayList<NodeInfo>> getContacts(){
        return this.contacts;
    }
    
    public UUID getUid(){
        return this.uid;
    }
}
