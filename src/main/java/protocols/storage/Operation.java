package protocols.storage;

import pt.unl.fct.di.novasys.network.data.Host;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class Operation {

    private static final String GET = "GET";
    private static final String INSERT = "INSERT";

    private boolean opType;
    private BigInteger id;
    private String name;
    private int hostIndex;
    private List<Host> hostList;
    private List<Host> notAlreadyAsked;
    
    public Operation(boolean opType, BigInteger id, String name) {
        this.opType=opType;
        this.id=id;
        this.name=name;
        hostIndex=0;
        hostList = new ArrayList<>();
        notAlreadyAsked = new ArrayList<>();
    }

    public String isOpType() {
        return opType?INSERT:GET;
    }

    public boolean getOpType(){ 
        return opType; 
    } //True if insert/Put; False if retrieve/Get

    public BigInteger getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public boolean nextHost(){
       return ++hostIndex < hostList.size();
    }

    public Host getHost() { 
        Host h = notAlreadyAsked.get(0);
        notAlreadyAsked.remove(h);
        return h; 
    }

    public boolean alreadyAskedAll(){
        return notAlreadyAsked.isEmpty();
    }

    public void setHostList(List<Host> lH) {
        hostList = lH;
        notAlreadyAsked = lH;
    }

    public boolean addHost(Host h){ 
        return hostList.add(h); 
    }
}
