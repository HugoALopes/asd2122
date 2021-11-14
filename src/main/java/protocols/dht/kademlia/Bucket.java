package protocols.dht.kademlia;

import java.math.BigInteger;
import java.util.ArrayList;

public class Bucket {

    private BigInteger min;
    private BigInteger max;
    private ArrayList<Node> nodes; 

    public Bucket(BigInteger min, BigInteger max){  
        this.min = min;
        this.max = max;
        nodes = new ArrayList<>(); 
    }
    
    public BigInteger getMin(){
        return this.min;
    }
    
    public BigInteger getMax(){
        return this.max;
    }

    public ArrayList<Node> getNodes(){
        return this.nodes;
    }

    public void addNode(Node n){
        nodes.add(n);
    }

    public void removeNode(Node n){
        nodes.remove(n);
    }

    public Boolean containsNode(Node n){
        return nodes.contains(n);
    }
}
