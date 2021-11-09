package protocols.dht.messages;

import pt.unl.fct.di.novasys.network.data.Host;
import java.util.*;

public class InformationGossip {
    private Map<Integer, Set<Host>> contacts;
    private Set<Host> agView;
    private Map<Integer, Host> fileTuples;

    public InformationGossip(Map<Integer, Set<Host>> contacts, Map<Integer, Host> fileTuples, Set<Host> agView) {
        this.contacts = contacts;
        this.agView = agView;
        this.fileTuples = fileTuples;
    }

    public Map<Integer, Host> getFileTuples() {
        return this.fileTuples;
    }

    public Set<Host> getAgView() {
        return this.agView;
    }

    public Map<Integer, Set<Host>> getContacts() {
        return this.contacts;
    }
}
