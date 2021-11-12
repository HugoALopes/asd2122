package protocols.dht.kelips.messages;

import pt.unl.fct.di.novasys.network.data.Host;

import java.math.BigInteger;
import java.util.*;

public class InformationGossip {
    private Map<Integer, Set<Host>> contacts;
    private Set<Host> agView;
    private Map<BigInteger, Host> fileTuples;

    public InformationGossip(Map<Integer, Set<Host>> contacts, Map<BigInteger, Host> fileTuples, Set<Host> agView) {
        this.contacts = contacts;
        this.agView = agView;
        this.fileTuples = fileTuples;
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
}
