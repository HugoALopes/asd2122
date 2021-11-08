package protocols.dht.kademlia;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.dht.Kelips;
import protocols.dht.kademlia.replies.KademliaFindNodeReply;
import protocols.dht.kademlia.replies.LookupResponse;
import protocols.dht.kademlia.requests.KademliaFindNodeRequest;
import protocols.dht.kademlia.requests.LookupRequest;
import protocols.storage.Storage;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HashGenerator;

public class Kademlia extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(Kelips.class);

    // Protocol information, to register in babel
    public final static short PROTOCOL_ID = 200;
    public final static String PROTOCOL_NAME = "kademlia";

    private Map<BigInteger, List<Node>> ongoing; //guarda para cada pedido de lookup uma lista com os nós dos quais estou à espera de resposta ao findNode
    private Map<BigInteger, List<Node>> queried; //lista de nós que ja responderam
    private Map<BigInteger, List<Node>> kclosestMap; //Guarda para cada pedido de lookup os k closest nós conhecidos até ao momento

    private Node my_node;
    private List<List<Node>> k_buckets_list;
    //Estas duas variáveis vêm do ficheiro de config
    private int alfa;
    private int k;

    public Kademlia(Host self, int alfa) throws HandlerRegistrationException{
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.alfa = alfa;
        k_buckets_list = new ArrayList<List<Node>>();
        my_node = new Node(self, HashGenerator.generateHash(self.toString()));

        ongoing = new HashMap<>();
        queried = new HashMap<>();
        kclosestMap = new HashMap<>();

        /*----------------------------- Register Message Handlers ----------------------------- */
        //TODO: alterar o 0 para o valor do channelId
        registerMessageHandler(0, KademliaFindNodeRequest.REQUEST_ID, this::uponFindNode, this::uponMsgFail);
        registerMessageHandler(0, KademliaFindNodeReply.REQUEST_ID, this::uponFindNodeReply, this::uponMsgFail);

        /*------------------------------ Register Reply Handlers ------------------------------ */
        //registerReplyHandler(LookupResponse.REPLY_ID, this::uponLookupReplyMessage);

        /*----------------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(LookupRequest.REQUEST_ID, this::uponLookupRequest);

        /*------------------------------------- TCPEvents ------------------------------------- */
        //TODO: alterar o 0 para o valor do channelId
        //TODO: faltam todos os tcp events
        registerChannelEventHandler(0, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {
        if (properties.contains("contact")) {
            try {
                String contact = properties.getProperty("contact");
                String[] hostElems = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                openConnection(contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + properties.getProperty("contact"));
                System.exit(-1);
            }
        }
    }

    /* --------------------------------- Messages ---------------------------- */
    //TODO
    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {

    }

    private void uponFindNode(KademliaFindNodeRequest msg, Host host, short destProto, int channelId) {
        List<Node> closest_nodes = find_node(msg.getIdToFind());
        insert_on_k_bucket(msg.getSender());
        KademliaFindNodeReply reply = new KademliaFindNodeReply(closest_nodes, msg.getIdToFind(), my_node);
        sendMessage(reply, msg.getSender().getHost());
    }

    private void uponFindNodeReply(KademliaFindNodeReply msg, Host host, short sourceProto, int channelId){
        BigInteger idToFind = msg.getIdToFind();

        if(!queried.containsKey(idToFind));
            queried.put(idToFind, new ArrayList<>());

        queried.get(idToFind).add(msg.getSender());
        ongoing.get(idToFind).remove(msg.getSender());
    
        List<Node> kclosestReturned = msg.getClosestNodes();
        List<Node> temp = new ArrayList<>(k);
        for(int i = 0; i < k; i++){
            insert_on_k_bucket(kclosestReturned.get(i));
            Node n = kclosestMap.get(idToFind).get(i);
            Node node = kclosestReturned.get(i);
            if(this.calculate_dist(node.getNodeId(), idToFind) < this.calculate_dist(n.getNodeId(), idToFind)) //Tenho de inserir 
                temp.add(i, node);
            else
                temp.add(i, n);
        }
        
        if(kclosestMap.get(idToFind) == temp){ //encontrei os knodes mais proximos
            LookupResponse reply = new LookupResponse(temp);
            sendReply(reply, Storage.PROTOCOL_ID); 
            ongoing.remove(idToFind);
            queried.remove(idToFind); 
            kclosestMap.remove(idToFind);
        } else {
            kclosestMap.replace(idToFind, temp);
            for(Node n: temp){
                if(!queried.get(idToFind).contains(n) && !ongoing.get(idToFind).contains(n)){ //ainda não contactei com este no
                    ongoing.get(idToFind).add(n);
                    sendMessage(new KademliaFindNodeRequest(idToFind, my_node), n.getHost());
                }
            }
        }
    }

    /* --------------------------------- Reply ---------------------------- 
    private void uponLookupReplyMessage(LookupResponse msg, short sourceProto){

    }*/

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponLookupRequest(LookupRequest lookupRequest, short sourceProto) {
        //E se eu tiver um lookup do mesmo ficheiro quase simultaneamente
        node_lookup(lookupRequest.getObjID());
    }

    /*--------------------------------- TCP ---------------------------------------- */
    // If a connection is successfully established, this event is triggered.
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Out Connection to {} is up.", peer);
        //Tenho de apanhar o node a partir do host
        insert_on_k_bucket(new Node(peer, HashGenerator.generateHash(peer.toString()))); //Adiciona o contacto ao k_bucket 
        node_lookup(my_node.getNodeId());
    }

    /* --------------------------------- Utils ---------------------------- */
    private void insert_on_k_bucket(Node node) {
        int distance = calculate_dist(node.getNodeId(), my_node.getNodeId());
        int i = (int) (Math.log(distance) / Math.log(2));

        List<Node> k_bucket = k_buckets_list.get(i);
        if (k_bucket == null)
            k_bucket = new ArrayList<Node>();

        if(k_bucket.contains(node)){ //colocar o no na cauda da lista
            k_bucket.remove(node);
            k_bucket.add(node); 
        } else
            k_bucket.add(node);

    }
 
    private List<Node> find_node(BigInteger node_id){
        int distance = calculate_dist(node_id, my_node.getNodeId());
        int i = (int) (Math.log(distance) / Math.log(2));
        List<Node> closest_nodes = new ArrayList<Node>(k); //Definir um comparador para que os nos fiquem organizados pela distancia

        for (int j = i; j < k_buckets_list.size() && closest_nodes.size() <= alfa; j++) {
            List<Node> k_bucket = k_buckets_list.get(j);
            if (k_bucket.size() != 0) {
                for (int m = 0; closest_nodes.size() <= k && m < k_bucket.size(); m++) {
                    closest_nodes.add(k_bucket.get(m));
                }
            }
        }
        return closest_nodes;
    }

    private int calculate_dist(BigInteger node1, BigInteger node2){
        return node2.xor(node1).intValue();
    }

    private void node_lookup(BigInteger id) {
        List<Node> kclosest = find_node(id); //list containing the k closest nodes

        //TODO: tenho de ordenar a lista
        kclosestMap.put(id, kclosest);

        ongoing.put(id, new ArrayList<>(alfa));
        for(int i = 0; i < alfa; i++){
            ongoing.get(id).add(kclosest.get(i));
            sendMessage(new KademliaFindNodeRequest(id, my_node), kclosest.get(i).getHost());
        }
    }
}
