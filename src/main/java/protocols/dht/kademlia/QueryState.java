package protocols.dht.kademlia;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class QueryState {

    private int k;

    private List<Node> ongoing; //guarda para cada pedido de lookup uma lista com os nós dos quais estou à espera de resposta ao findNode
    private List<Node> queried; //lista de nós que ja responderam
    private List<Node> kclosest; //Guarda para cada pedido de lookup os k closest nós conhecidos até ao momento

    public QueryState(List<Node> kclosest){
        this.kclosest = kclosest;
        ongoing = new ArrayList<>();
        queried = new ArrayList<>();
        k = kclosest.size();   
    }

    public boolean hasStabilised(){
        return queried.containsAll(kclosest);
    }

    public boolean alreadyQueried(Node n){
        return queried.contains(n);
    }

    public boolean stillOngoing(Node n){
        return ongoing.contains(n);
    }

    public List<Node> getKclosest(){
        return kclosest;
    }

    public void sendFindNodeRequest(Node n){
        ongoing.add(n);
    }

    public void receivedFindNodeReply(Node n){
        queried.add(n);
        ongoing.remove(n);
    }

    public void updateKclosest(Node other, BigInteger idToFind){ //se calhar o idToFind pode ser uma variavel global
        for(int i = 0; i < k; i++){
            Node node = kclosest.get(i);
            if(this.calculate_dist(node.getNodeId(), idToFind) < calculate_dist(other.getNodeId(), idToFind)){ //Tenho de inserir 
                kclosest.set(i, other);
                break;
            } 
        }

    }
    
    private int calculate_dist(BigInteger node1, BigInteger node2){
        return node2.xor(node1).intValue();
    } 
}
