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

    private Map<BigInteger, QueryState> queriesByIdToFind;

    private Node my_node;
    private List<List<Node>> k_buckets_list;
    private int alfa;
    private int k;

    public Kademlia(Host self, int alfa) throws HandlerRegistrationException{
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.alfa = alfa;
        k_buckets_list = new ArrayList<List<Node>>();
        my_node = new Node(self, HashGenerator.generateHash(self.toString()));

        queriesByIdToFind = new HashMap<>();

        /*----------------------------- Register Message Handlers ----------------------------- */
        //TODO: alterar o 0 para o valor do channelId
        registerMessageHandler(0, KademliaFindNodeRequest.REQUEST_ID, this::uponFindNode, this::uponMsgFail);
        registerMessageHandler(0, KademliaFindNodeReply.REQUEST_ID, this::uponFindNodeReply, this::uponMsgFail);

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
                alfa = Integer.parseInt(properties.getProperty("alfaValue"));
                k = Integer.parseInt(properties.getProperty("kValue"));
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

        QueryState query = queriesByIdToFind.get(idToFind);
        query.receivedFindNodeReply(msg.getSender());
    
        List<Node> kclosestReturned = msg.getClosestNodes();
        for(int i = 0; i < k; i++){
            insert_on_k_bucket(kclosestReturned.get(i));
            query.updateKclosest(kclosestReturned.get(i), idToFind);
        }

        if(query.hasStabilised()){ //encontrei os knodes mais proximos
            LookupResponse reply = new LookupResponse(query.getKclosest());
            sendReply(reply, Storage.PROTOCOL_ID); 
            queriesByIdToFind.remove(idToFind);
        } else {
            List<Node> kclosest = query.getKclosest();
            for(Node n: kclosest){
                if(!query.alreadyQueried(n) && !query.stillOngoing(n)){ //ainda não contactei com este no
                    query.sendFindNodeRequest(n);
                    sendMessage(new KademliaFindNodeRequest(idToFind, my_node), n.getHost());
                }
            } 
        }
    }

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

    private void node_lookup(BigInteger id) {
        List<Node> kclosest = find_node(id); //list containing the k closest nodes

        QueryState query = new QueryState(kclosest);

        for(int i = 0; i < alfa; i++){
            query.sendFindNodeRequest(kclosest.get(i));
            sendMessage(new KademliaFindNodeRequest(id, my_node), kclosest.get(i).getHost());
        }

        queriesByIdToFind.put(id, query);
    }

    private int calculate_dist(BigInteger node1, BigInteger node2){
        return node2.xor(node1).intValue();
    }
}
