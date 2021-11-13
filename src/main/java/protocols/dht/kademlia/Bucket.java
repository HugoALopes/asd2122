package protocols.dht.kademlia;

import java.math.BigInteger;
import java.util.ArrayList;

import pt.unl.fct.di.novasys.network.data.Host;

public class Bucket {

    private BigInteger min;
    private BigInteger max;
    private ArrayList<Node> hosts; 

    public Bucket(BigInteger min, BigInteger max){  
        this.min = min;
        this.max = max;
        hosts = new ArrayList<>(); 
    }
    
    public BigInteger getMin(){
        return this.min;
    }
    
    public BigInteger getMax(){
        return this.max;
    }

    public ArrayList<Node> getHosts(){
        return this.hosts;
    }

    public void addHost(Node h){
        hosts.add(h);
    }

    public void removeHost(Node h){
        hosts.remove(h);
    }

    public Boolean containsHost(Node h){
        return this.hosts.contains(h);
    }
}
