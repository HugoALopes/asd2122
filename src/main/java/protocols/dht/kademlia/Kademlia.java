package protocols.dht.kademlia;

import membership.common.ChannelCreated;
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

public class Kademlia extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(Kademlia.class);

    // Protocol information, to register in babel
    public final static short PROTOCOL_ID = 1100;
    public final static String PROTOCOL_NAME = "kademlia";

    private Map<BigInteger, QueryState> queriesByIdToFind;

    private Node my_node;
    private List<Bucket> k_buckets_list;
    private int alfa, k, channelId;

    public Kademlia(Host self, Properties props) throws HandlerRegistrationException, IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        k_buckets_list = new ArrayList<Bucket>();
        my_node = new Node(self, HashGenerator.generateHash(self.toString()));
        queriesByIdToFind = new HashMap<>();
        alfa = Integer.parseInt(props.getProperty("alfaValue"));
        k = Integer.parseInt(props.getProperty("kValue"));

        String cMetricsInterval = props.getProperty("channel_metrics_interval", "10000"); // 10 seconds

        // Create a properties object to setup channel-specific properties. See the
        // channel description for more details.
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address")); // The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("port")); // The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); // The interval to receive channel
                                                                                     // metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); // Heartbeats interval for established
                                                                             // connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); // Time passed without heartbeats until
                                                                              // closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); // TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); // Create the channel with the given properties

        /*----------------------------- Register Message Handlers ----------------------------- */
        registerMessageHandler(channelId, KademliaFindNodeRequest.MESSAGE_ID, this::uponFindNode, this::uponMsgFail);
        registerMessageHandler(channelId, KademliaFindNodeReply.MESSAGE_ID, this::uponFindNodeReply, this::uponMsgFail);

        /*--------------------- Register Message Serializers ----------------------------- */
        // registerMessageSerializer(channelId, KademliaFindNodeRequest.MESSAGE_ID,
        // KademliaFindNodeRequest.serializer);
        // registerMessageSerializer(channelId, KademliaFindNodeReply.MESSAGE_ID,
        // KademliaFindNodeReply.serializer);

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
        triggerNotification(new ChannelCreated(channelId));
        if (properties.containsKey("contact")) {
            logger.info("Contains contact");
            try {
                String contact = properties.getProperty("contact");
                String[] hostElems = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                openConnection(contactHost);
                logger.info("Contains contact");
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + properties.getProperty("contact"));
                System.exit(-1);
            }
        }
        logger.info("Nao Contains contact");
    }

    /* --------------------------------- Messages ---------------------------- */
    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {

    }

    private void uponFindNode(KademliaFindNodeRequest msg, Host host, short destProto, int channelId) {
        List<Node> closest_nodes = find_node(msg.getIdToFind());
        insert_on_k_bucket(msg.getSender());
        KademliaFindNodeReply reply = new KademliaFindNodeReply(msg.getUid(), closest_nodes, msg.getIdToFind(),
                my_node);
        sendMessage(reply, msg.getSender().getHost());
    }

    private void uponFindNodeReply(KademliaFindNodeReply msg, Host host, short sourceProto, int channelId) {
        BigInteger idToFind = msg.getIdToFind();

        QueryState query = queriesByIdToFind.get(idToFind);
        query.receivedFindNodeReply(msg.getSender());

        List<Node> kclosestReturned = msg.getClosestNodes();
        for (int i = 0; i < k; i++) {
            insert_on_k_bucket(kclosestReturned.get(i));
            query.updateKclosest(kclosestReturned.get(i), idToFind);
        }

        if (query.hasStabilised() && msg.getUid() != null) { // encontrei os knodes mais proximos
            LookupResponse reply = new LookupResponse(msg.getUid(), msg.getIdToFind(), query.getKHosts());
            sendReply(reply, Storage.PROTOCOL_ID);
            queriesByIdToFind.remove(idToFind);
        } else {
            List<Node> kclosest = query.getKclosest();
            for (Node n : kclosest) {
                if (!query.alreadyQueried(n) && !query.stillOngoing(n)) { // ainda não contactei com este no
                    query.sendFindNodeRequest(n);
                    sendMessage(new KademliaFindNodeRequest(msg.getUid(), idToFind, my_node), n.getHost());
                }
            }
        }
    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponLookupRequest(LookupRequest lookupRequest, short sourceProto) {
        // E se eu tiver um lookup do mesmo ficheiro quase simultaneamente
        node_lookup(lookupRequest.getObjID(), lookupRequest.getRequestUID());
    }

    /*--------------------------------- TCP ---------------------------------------- */
    // If a connection is successfully established, this event is triggered.
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.info("Out Connection to {} is up.", peer);
        // Tenho de apanhar o node a partir do host
        insert_on_k_bucket(new Node(peer, HashGenerator.generateHash(peer.toString()))); // Adiciona o contacto ao
                                                                                         // k_bucket
        node_lookup(my_node.getNodeId(), null);
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.info("Out Connection to {} is down cause {}", peer, event.getCause());

        Node n = new Node(peer, HashGenerator.generateHash(peer.toString()));
        this.remove_from_k_bucket(n);
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} failed cause: {}", peer, event.getCause());
    }

    // A connection someone established to me is disconnected.
    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.info("In Connection to {} is down cause {}", peer, event.getCause());

        Node n = new Node(peer, HashGenerator.generateHash(peer.toString()));
        this.remove_from_k_bucket(n);
    }

    /* --------------------------------- Utils ---------------------------- */
    private void insert_on_k_bucket(Node node) {
        BigInteger distance = calculate_dist(node.getNodeId(), my_node.getNodeId());

        Bucket insert_bucket = getBucket(distance);

        if (insert_bucket.containsHost(node)) { // colocar o no na cauda da lista
            insert_bucket.removeHost(node);
        }
        insert_bucket.addHost(node);

        ArrayList<Node> hosts = insert_bucket.getHosts();
        if(hosts.size() > k  && hosts.contains(this.my_node)){
            this.divideBucket(insert_bucket);
            logger.info("k_bucket divided.");
        }
        
        logger.info("Node added.");
    }

    private void remove_from_k_bucket(Node node) {
        BigInteger distance = calculate_dist(node.getNodeId(), my_node.getNodeId());

        Bucket bucket_remove = getBucket(distance);

        if (bucket_remove.containsHost(node)){
            bucket_remove.removeHost(node);
        }
    }

    private List<Node> find_node(BigInteger node_id) {
        BigInteger distance = calculate_dist(node_id, my_node.getNodeId());
        
        List<Node> closest_nodes = new ArrayList<Node>(k); // Definir um comparador para que os nos fiquem organizados
        
     
        Bucket b = getBucket(distance);
        for(Node n: b.getHosts()){
            if(closest_nodes.size() <= k)
                closest_nodes.add(n);
            }
  
        while(closest_nodes.size() < k){
            for(Bucket bucket: this.k_buckets_list){
                if(b.getMax().compareTo(bucket.getMin().subtract(new BigInteger("1"))) == 0){
                    for(Node h: bucket.getHosts()){
                        closest_nodes.add(h);
                        if(closest_nodes.size() >= k)
                            break;
                    }
                }
            }
        }

        return closest_nodes;
    }

    private void node_lookup(BigInteger id, UUID mid) {
        List<Node> kclosest = find_node(id); // list containing the k closest nodes
        logger.info("pre null pointer: {}", kclosest.size());
        QueryState query = new QueryState(kclosest);

        for (int i = 0; i < alfa && i < kclosest.size(); i++) {
            query.sendFindNodeRequest(kclosest.get(i));
            sendMessage(new KademliaFindNodeRequest(mid, id, my_node), kclosest.get(i).getHost());
        }

        if (kclosest.size() == 0) {
            ArrayList<Host> myHost = new ArrayList<Host>();
            myHost.add(this.my_node.getHost());
            sendReply(new LookupResponse(mid, id, myHost), Storage.PROTOCOL_ID);
        }

        queriesByIdToFind.put(id, query);
    }

    private BigInteger calculate_dist(BigInteger node1, BigInteger node2) {
        return node2.xor(node1);
    }

    private Bucket getBucket(BigInteger dist){
        Bucket result = null;
        
        for(Bucket b: this.k_buckets_list){
            if((dist.compareTo(b.getMax()) == -1) && ((dist.compareTo(b.getMin()) == 1) || dist.compareTo(b.getMin()) == 0))
                result = b;
        }
        
        return result;
    }

    //Nao percebo exatamente como dividir
    private void divideBucket(Bucket b){
        BigInteger dist_interval_values = b.getMax().subtract(b.getMin());
        Bucket new_b = new Bucket(dist_interval_values.divide(new BigInteger("2")), b.getMax());
        Bucket new_b_2 = new Bucket(b.getMin(), dist_interval_values.divide(new BigInteger("2")));

        ArrayList<Node> hosts = b.getHosts();
        for(Node h: hosts){
            BigInteger dist = calculate_dist(h.getNodeId(), this.my_node.getNodeId());
            if(dist.compareTo(new_b.getMax()) == 1)
                new_b_2.addHost(h);
            else
                new_b.addHost(h);
        }

        this.k_buckets_list.remove(b);
        this.k_buckets_list.add(new_b);
        this.k_buckets_list.add(new_b_2);
    }
}
