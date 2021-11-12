package protocols.dht.kademlia;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.kademlia.messages.KademliaFindNodeReply;
import protocols.dht.kademlia.messages.KademliaFindNodeRequest;
import protocols.dht.replies.LookupResponse;
import protocols.dht.requests.LookupRequest;
import protocols.storage.Storage;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.*;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;

import utils.HashGenerator;
import utils.NodeToHostList;

public class Kademlia extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(Kademlia.class);

    // Protocol information, to register in babel
    public final static short PROTOCOL_ID = 1100;
    public final static String PROTOCOL_NAME = "kademlia";

    private Map<BigInteger, QueryState> queriesByIdToFind;

    private Node my_node;
    private List<List<Node>> k_buckets_list;
    private int alfa;
    private int k;

    public Kademlia(Host self, Properties props) throws HandlerRegistrationException, IOException{
        super(PROTOCOL_NAME, PROTOCOL_ID);
        k_buckets_list = new ArrayList<List<Node>>();
        my_node = new Node(self, HashGenerator.generateHash(self.toString()));
        queriesByIdToFind = new HashMap<>();
        alfa = Integer.parseInt(props.getProperty("alfaValue"));
        k = Integer.parseInt(props.getProperty("kValue"));

        String cMetricsInterval = props.getProperty("channel_metrics_interval", "10000"); //10 seconds

        //Create a properties object to setup channel-specific properties. See the channel description for more details.
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address")); //The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("port")); //The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); //The interval to receive channel metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); //Heartbeats interval for established connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); //Time passed without heartbeats until closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); //TCP connect timeout
        int channelId = createChannel(TCPChannel.NAME, channelProps); //Create the channel with the given properties

        /*----------------------------- Register Message Handlers ----------------------------- */
        registerMessageHandler(channelId, KademliaFindNodeRequest.REQUEST_ID, this::uponFindNode, this::uponMsgFail);
        registerMessageHandler(channelId, KademliaFindNodeReply.REQUEST_ID, this::uponFindNodeReply, this::uponMsgFail);

        /*----------------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(LookupRequest.REQUEST_ID, this::uponLookupRequest);

        /*------------------------------------- TCPEvents ------------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
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
            List<Host> hostList = NodeToHostList.convert(query.getKclosest());
            LookupResponse reply = new LookupResponse(null, null, hostList);
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
        //TODO - Hugo - add uuid
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

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Out Connection to {} is down cause {}", peer, event.getCause());

        Node n = new Node(peer, HashGenerator.generateHash(peer.toString()));
        this.remove_from_k_bucket(n);
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} failed cause: {}", peer, event.getCause());
    }


    //A connection someone established to me is disconnected.
    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.debug("In Connection to {} is down cause {}", peer, event.getCause());

        Node n = new Node(peer, HashGenerator.generateHash(peer.toString()));
        this.remove_from_k_bucket(n);
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

    private void remove_from_k_bucket(Node node){
        int distance = calculate_dist(node.getNodeId(), my_node.getNodeId());
        int i = (int) (Math.log(distance) / Math.log(2));

        List<Node> k_bucket = k_buckets_list.get(i);
        if(k_bucket.contains(node))
            k_bucket.remove(node);

    }
 
    private List<Node> find_node(BigInteger node_id){
        int distance = calculate_dist(node_id, my_node.getNodeId());
        int i = (int) (Math.log(distance) / Math.log(2));
        List<Node> closest_nodes = new ArrayList(k); //Definir um comparador para que os nos fiquem organizados pela distancia

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
