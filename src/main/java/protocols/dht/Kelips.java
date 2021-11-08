package protocols.dht;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import membership.common.ChannelCreated;
import membership.common.NeighbourDown;
import protocols.Broadcast.FloodBroadcast;
import protocols.Broadcast.common.BroadcastRequest;
import protocols.Broadcast.common.DeliverNotification;
import protocols.dht.messages.KelipsInformRequest;
import protocols.dht.messages.KelipsJoinRequest;
import protocols.dht.messages.KelipsJoinReply;
import protocols.dht.replies.LookupResponse;
import protocols.dht.requests.LookupRequest;
import protocols.timers.*;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.ChannelMetrics;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionUp;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HashGenerator;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

import static utils.HashGenerator.generateHash;

public class Kelips extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(Kelips.class);

    //enum para os tempos de razao para a conexao estar pendente
    public enum Reason {
        NEW_JOIN, JOIN, INFORM, INFORM_DONE, OPEN_CONNECTION, RETRYCON
    }

    // Protocol information, to register in babel
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "Kelips";
    private int channelId;


    //numero de contactos 
    private int agNum = 4;

    //informaçao do proprio
    private final Host me;
    private final int myAG;
    private Random rnd;

    //Conexoes pendentes ainda nao estabelecidas
    private final Map<NodeInfo, Set<Reason>> pending;

    //Soft state do no
    private Set<NodeInfo> agView;
    private Map<Integer, ArrayList<NodeInfo>> contacts;
    private Map<Integer, NodeInfo> filetuples;
    
 

    public Kelips(Host self, Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        
        this.me = self;
        rnd = new Random();
        this.agNum = Integer.parseInt(props.getProperty("agNum"));
        myAG = generateHash(me.toString()).intValue() % this.agNum; // TODO - check this
        
        channelId = 0;

        pending = new HashMap<>();

        filetuples = new HashMap<>();
        contacts = new HashMap<>();
        agView = new HashSet<>();
       
    
        
        String cMetricsInterval = props.getProperty("channel_metrics_interval", "10000"); //10 seconds

        //Create a properties object to setup channel-specific properties. See the channel description for more details.
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address")); //The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("port")); //The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); //The interval to receive channel metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); //Heartbeats interval for established connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); //Time passed without heartbeats until closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); //TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); //Create the channel with the given properties

        /*--------------------- Register Message Handlers ----------------------------- */
        registerMessageHandler(channelId, KelipsJoinRequest.REQUEST_ID, this::uponJoinMessage, this::uponMsgFail);
        registerMessageHandler(channelId, KelipsJoinReply.REQUEST_ID, this::uponJoinReplyMessage, this::uponMsgFail);
        registerMessageHandler(channelId, KelipsInformRequest.REQUEST_ID, this::uponInformMessage, this::uponMsgFail);

        /*--------------------- Register Reply Handlers ----------------------------- */
        registerReplyHandler(LookupResponse.REPLY_ID, this::uponLookupReplyMessage);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(LookupRequest.REQUEST_ID, this::uponLookupRequest);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTime);

        /*--------------------- TCPEvents ----------------------------- */
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /*--------------------- Timers ----------------------------- */
        registerTimerHandler(GossipTimer.TIMER_ID, this::uponInfoTime);

        /*--------------------- Notifications subscribed ----------------------------- */
        subscribeNotification(DeliverNotification.NOTIFICATION_ID, this::uponDeliver);
    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {
        triggerNotification(new ChannelCreated(channelId));
        if (properties.contains("contact")) {
            try {
                String contact = properties.getProperty("contact");
                String[] hostElems = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                NodeInfo node = new NodeInfo(contactHost);
                connect(node, Reason.NEW_JOIN);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + properties.getProperty("contact"));
                System.exit(-1);
            }
        }

        setupPeriodicTimer(new GossipTimer(), 5000, 5000);
    }

    /*--------------------- Notifications subscribed ----------------------------- */
    private void uponDeliver(DeliverNotification not, short sourceProto){
        byte[] msg = not.getMsg();
        String code = new String(msg, StandardCharsets.US_ASCII);

        BigInteger hash = HashGenerator.generateHash(peer.toString());
        int fromID = (hash.intValue() % this.agNum);


        switch(code){
            case "d":
                removeContact(fromID, peer);

        }


    }


    /*--------------------------------- TCP ---------------------------------------- */
    // If a connection is successfully established, this event is triggered.
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
       
        triggerNotification(new NeighbourDown(peer));
        NodeInfo node = new NodeInfo(peer);
        Set<Reason> reasons = pending.remove(node);
        logger.debug("Out Connection to {} is up.", peer);
        if (reasons != null) {
            for(Reason reason: reasons){
                switch (reason) {
                    case NEW_JOIN: // SEND JOIN REQUEST DE UM NOVO NO
                        sendMessage(new KelipsJoinRequest(this.me), peer);
                        break;
                    case JOIN: //QUANDO UM NO RECEBE UM JOIN REQUEST ENVIA A SUA VIEW E A SI MESMO
                        KelipsJoinReply reply = new KelipsJoinReply(contacts, this.me, this.filetuples, this.agView);
                        sendMessage(reply, peer);
                        break;
                    case INFORM: //DAR O NOVO NO A CONHECER AOS RESTANTES
                        KelipsInformRequest msg = new KelipsInformRequest();
                        sendMessage(msg, peer);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Out Connection to {} is down cause {}", peer, event.getCause());
        
        BigInteger hash = HashGenerator.generateHash(peer.toString());
        int fromID = (hash.intValue() % this.agNum);
        this.removeContact(fromID, peer);
        triggerNotification(new NeighbourDown(peer));
        this.uponBroadcastTimer()
        //possivelmente enviar para todos que este no ja nao esta up
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} failed cause: {}", peer, event.getCause());

        BigInteger hash = HashGenerator.generateHash(peer.toString());
        int fromID = (hash.intValue() % this.agNum);
        this.removeContact(fromID, peer);
    }


    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.debug("In Connection from {} is up", peer);
    }

    //A connection someone established to me is disconnected.
    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.debug("In Connection to {} is down cause {}", peer, event.getCause());

        BigInteger hash = HashGenerator.generateHash(peer.toString());
        int fromID = (hash.intValue() % this.agNum);
        this.removeContact(fromID, peer);
    }


    /* --------------------------------- Reply ---------------------------- */
    private void uponLookupReplyMessage(LookupResponse msg, short sourceProto){

    }

    /* --------------------------------- Messages ---------------------------- */
    private void uponInformMessage(KelipsInformRequest msg, Host from, short sourceProto, int channelId) {
        BigInteger hash = HashGenerator.generateHash(from.toString());
        int fromID = (hash.intValue() % this.agNum);
        //Sao do mesmo afinnity group
        NodeInfo node =  new NodeInfo(from);
        if(fromID == myAG){
            agView.add(node);
        } //Sao de grupos diferentes
        else{
            ArrayList<NodeInfo> aux = contacts.get(fromID);
            if(aux == null){
                aux = new ArrayList<NodeInfo>();
                aux.add(node);
            }
            else if(aux.size() < this.agNum){
                aux.add(node);
            }
            contacts.put(fromID, aux);
        }   
        connect(node, Reason.INFORM_DONE);
    }

    private void uponJoinReplyMessage(KelipsJoinReply msg, Host from, short sourceProto, int channelId) {
        BigInteger hash = HashGenerator.generateHash(from.toString());
        int fromID = (hash.intValue() % this.agNum);

        NodeInfo p = null;

        this.contacts = msg.getContacts();

        if(!contacts.containsKey(this.myAG)){
            this.agView = msg.getAgView();
            this.filetuples = msg.getFileTuples();
        }else{
            ArrayList<NodeInfo> aux = contacts.get(this.myAG);
            int index = (int)(Math.random() * aux.size());
            p = aux.get(index);
        }

        connect(p, Reason.OPEN_CONNECTION);
        
        if(p != null)
            connect(p, Reason.JOIN);
    }

    private void uponJoinMessage(KelipsJoinRequest msg, Host from, short sourceProto, int channelId) {
        BigInteger hash = HashGenerator.generateHash(from.toString());
        int fromID = (hash.intValue() % this.agNum);
        
        NodeInfo node = new NodeInfo(from);
        
        //Sao do mesmo afinnity group
        if(fromID == myAG){
            agView.add(node);
        } //Sao de grupos diferentes
        else{
            ArrayList<NodeInfo> aux = contacts.get(fromID);
            if(aux == null){
                aux = new ArrayList<NodeInfo>();
                aux.add(node);
            }
            else if(aux.size() < this.agNum){
                aux.add(node);
            }
            contacts.put(fromID, aux);
        }   
        connect(node, Reason.JOIN);
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
        Throwable throwable, int channelId){

    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponLookupRequest(LookupRequest lookupRequest, short sourceProto) {
        // // TODO - check bigInteger to int
        // int fAG = lookupRequest.getObjID().intValue() % agNum;
        // Host host;
        // if (fAG == myAG) {
        //     host = filetuples.get(lookupRequest.getObjID());

        //     if (host == null) {
        //         // TODO - host <- gossip ( fAG, (GETHOST, id) )
        //     }

        // } else { // file does not belong my AG
        //     //Host contact = contacts.get(fAG)[rnd.nextInt(contacts.get(fAG).length)];

        //     // TODO - send msg - GETHOST
        //     GetHostMessage ghMsg = new GetHostMessage(null, lookupRequest.getObjID());
        //     //sendMessage(ghMsg, contact);
        // }
    }



    /* --------------------------------- Metrics ---------------------------- */

    // If we setup the InfoTimer in the constructor, this event will be triggered
    // periodically.
    // We are simply printing some information to present during runtime.
    private void uponInfoTime(InfoTimer timer, long timerId) {
        // TODO - log stuff
        /*
         * StringBuilder sb = new StringBuilder("Membership Metrics:\n");
         * sb.append("Membership: ").append(membership).append("\n");
         * sb.append("PendingMembership: ").append(pending).append("\n"); //getMetrics
         * returns an object with the number of events of each type processed by this
         * protocol. //It may or may not be useful to you, but at least you know it
         * exists. sb.append(getMetrics()); logger.info(sb);
         */
    }

    // If we passed a value >0 in the METRICS_INTERVAL_KEY property of the channel,
    // this event will be triggered
    // periodically by the channel. This is NOT a protocol timer, but a channel
    // event.
    // Again, we are just showing some of the information you can get from the
    // channel, and use how you see fit.
    // "getInConnections" and "getOutConnections" returns the currently established
    // connection to/from me.
    // "getOldInConnections" and "getOldOutConnections" returns connections that
    // have already been closed.
    private void uponChannelMetrics(ChannelMetrics event, int channelId) {
        StringBuilder sb = new StringBuilder("Channel Metrics:\n");
        sb.append("In channels:\n");
        event.getInConnections()
                .forEach(c -> sb.append(
                        String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n", c.getPeer(), c.getSentAppMessages(),
                                c.getSentAppBytes(), c.getReceivedAppMessages(), c.getReceivedAppBytes())));
        event.getOldInConnections()
                .forEach(c -> sb.append(
                        String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n", c.getPeer(), c.getSentAppMessages(),
                                c.getSentAppBytes(), c.getReceivedAppMessages(), c.getReceivedAppBytes())));
        sb.append("Out channels:\n");
        event.getOutConnections()
                .forEach(c -> sb.append(
                        String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n", c.getPeer(), c.getSentAppMessages(),
                                c.getSentAppBytes(), c.getReceivedAppMessages(), c.getReceivedAppBytes())));
        event.getOldOutConnections()
                .forEach(c -> sb.append(
                        String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n", c.getPeer(), c.getSentAppMessages(),
                                c.getSentAppBytes(), c.getReceivedAppMessages(), c.getReceivedAppBytes())));
        sb.setLength(sb.length() - 1);
        logger.info(sb);
    }

    /* --------------------------------- Utils ---------------------------- */
    private void connect(NodeInfo peer, Reason reason) {
        if(pending.containsKey(peer)){
            Set<Reason> set = pending.get(peer);
            set.add(reason);
            pending.replace(peer, set);
        }else{
            Set<Reason> set = new HashSet<>();
            set.add(reason);
            pending.put(peer, set);
        }
        openConnection(peer.getHost());
    }

    private void removeContact(int fromID, Host peer){
        if(fromID == myAG){
            for(NodeInfo n: agView){
                if(n.getHost().equals(peer))
                    agView.remove(n);
            }
        }else{
            ArrayList<NodeInfo> aux = contacts.get(fromID);
            for(NodeInfo n: aux){
                if(n.getHost().equals(peer))
                    aux.remove(n); 
            }
        }  
    }

    private void uponBroadcastTimer(String msg) {
        //Upon triggering the broadcast timer, create a new message
        
        //ASCII encodes each character as 1 byte
        byte[] payload = msg.getBytes(StandardCharsets.US_ASCII);

        BroadcastRequest request = new BroadcastRequest(UUID.randomUUID(), this.me, payload);
        
        logger.info("Sending: {} - {} ({})", request.getMsgId(), msg, payload.length);
        //And send it to the dissemination protocol
        sendRequest(request, FloodBroadcast.PROTOCOL_ID);
    }

}
