package protocols.dht.kelips;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import membership.common.ChannelCreated;
import membership.common.NeighbourDown;
import protocols.broadcast.*;
import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;
import protocols.dht.kelips.messages.*;
import protocols.dht.replies.LookupResponse;
import protocols.dht.requests.LookupRequest;
import protocols.storage.Storage;
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
import utils.Serializer;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.net.InetAddress;

import static utils.HashGenerator.generateHash;

@SuppressWarnings({"unused", "Convert2Diamond"})
public class Kelips extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(Kelips.class);

    //enum para os tempos de razao para a conexao estar pendente
    public enum Reason {
        NEW_JOIN, JOIN, INFORM, INFORM_DONE, CONTACT_AG_FILE
    }

    // Protocol information, to register in babel
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "Kelips";
    private int channelId;


    //#affinity groups
    private final int agNum;
    private int numContacts;

    //informaçao do proprio
    private final Host me;
    private final int myAG;

    //Conexoes pendentes ainda nao estabelecidas
    private final Map<Host, Set<Reason>> pending;
    private final Map<UUID, Set<Host>> ongoinglookUp;

    //Soft state do no
    private Set<Host> agView;
    private Map<Integer, Set<Host>> contacts;
    private Map<BigInteger, Host> filetuples;

    private Map<Integer, Set<Host>> candidates;

    public Kelips(Host self, Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.numContacts = 5;
        this.me = self;
        this.agNum = Integer.parseInt(props.getProperty("agNum"));
        myAG = generateHash(me.toString()).mod(BigInteger.valueOf(agNum)).intValueExact();

        pending = new HashMap<>();

        filetuples = new HashMap<>();
        contacts = new HashMap<>();
        agView = new HashSet<>();
        candidates = new HashMap<>();
        ongoinglookUp = new HashMap<>();

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
        registerMessageHandler(channelId, KelipsJoinRequest.MESSAGE_ID, this::uponJoinMessage, this::uponMsgFail);
        registerMessageHandler(channelId, KelipsJoinReply.MESSAGE_ID, this::uponJoinReplyMessage, this::uponMsgFail);
        registerMessageHandler(channelId, KelipsInformRequest.MESSAGE_ID, this::uponInformMessage, this::uponMsgFail);
        registerMessageHandler(channelId, GetFileMessage.MESSAGE_ID, this::uponGetFileMessage, this::uponMsgFail);
        registerMessageHandler(channelId, GetDiffAgFileMessage.MESSAGE_ID, this::uponGetDiffAgFileMessage, this::uponMsgFail);
        registerMessageHandler(channelId, GetFileReply.MESSAGE_ID, this::uponLookupReplyMessage, this::uponMsgFail);

        /*--------------------- Register Message Serializers ----------------------------- */
        registerMessageSerializer(channelId, KelipsJoinRequest.MESSAGE_ID, KelipsJoinRequest.serializer);
        registerMessageSerializer(channelId, KelipsJoinReply.MESSAGE_ID, KelipsJoinReply.serializer);
        registerMessageSerializer(channelId, KelipsInformRequest.MESSAGE_ID, KelipsInformRequest.serializer);
        registerMessageSerializer(channelId, GetFileMessage.MESSAGE_ID, GetFileMessage.serializer);
        registerMessageSerializer(channelId, GetDiffAgFileMessage.MESSAGE_ID, GetDiffAgFileMessage.serializer);
        registerMessageSerializer(channelId, GetFileReply.MESSAGE_ID, GetFileReply.serializer);


        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(LookupRequest.REQUEST_ID, this::uponLookupRequest);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTime);
        //registerTimerHandler(GossipTimer.TIMER_ID, this::broadcastRequest);


        /*--------------------- TCPEvents ----------------------------- */
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /*--------------------- Notifications subscribed ----------------------------- */
        //subscribeNotification(DeliverNotification.NOTIFICATION_ID, this::uponDeliver);
    }

    @Override
    public void init(Properties properties) {
        triggerNotification(new ChannelCreated(channelId));
        if (properties.containsKey("contact")) {
            try {
                String contact = properties.getProperty("contact");
                logger.info("{} ->{} is contact",me, contact);
                String[] hostElems = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                if (contactHost.equals(me)) {
                    agView.add(me);
                    return;
                }
                connect(contactHost, Reason.NEW_JOIN);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + properties.getProperty("contact"));
                System.exit(-1);
            }
        } else
            logger.info("{} -> Do not Contains contact", me);
        agView.add(me);
        //setupPeriodicTimer(new GossipTimer(), 5000, 20000);
    }

    /*--------------------- Notifications subscribed ----------------------------- */
    private void uponDeliver(DeliverNotification not, short sourceProto) {
        Host sender = not.getSender();

        BigInteger hash = HashGenerator.generateHash(sender.toString());
        int fromID = (hash.intValue() % this.agNum);

        InformationGossip ig = Serializer.deserialize(not.getMsg());
        Map<Integer, Set<Host>> contactsIG = ig.getContacts();
        Set<Host> view = ig.getAgView();

        if (fromID == this.myAG) {
            for (Host h : view) {
                if (!this.agView.contains(h))
                    view.add(h);
            }
        } else {
            contactsIG.put(fromID, view);
        }

        for (Integer key : contactsIG.keySet()) {
            if (!contacts.containsKey(key) && key != this.myAG) {
                contacts.put(key, contactsIG.get(key));
            } else if (contacts.containsKey(key) && key != this.myAG) {
                Set<Host> aux = contacts.get(key);
                Set<Host> aux2 = contactsIG.get(key);
                Iterator<Host> it = aux2.iterator();
                while (it.hasNext() && aux.size() < this.numContacts) {
                    Host igH = it.next();
                    aux.add(igH);
                    aux2.remove(igH);
                }
                contacts.put(key, aux);
                contactsIG.put(key, aux2);
            }
        }

        for (BigInteger key : ig.getFileTuples().keySet()) {
            if (!filetuples.containsKey(key))
                filetuples.put(key, ig.getFileTuples().get(key));
        }

        candidates = contactsIG;
    }

    /*--------------------------------- TCP ---------------------------------------- */
    // If a connection is successfully established, this event is triggered.
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.info("In uponOutConnectionUp");
        Host peer = event.getNode();
        triggerNotification(new NeighbourDown(peer));
        Set<Reason> reasons = pending.remove(peer);
        logger.debug("Out Connection to {} is up.", peer);
        if (reasons != null) {
            for (Reason reason : reasons) {
                switch (reason) {
                    case NEW_JOIN: // SEND JOIN REQUEST DE UM NOVO NO
                        sendMessage(new KelipsJoinRequest(this.me), peer);
                        break;
                    case JOIN: //QUANDO UM NO RECEBE UM JOIN REQUEST ENVIA A SUA VIEW E A SI MESMO
                        KelipsJoinReply reply = new KelipsJoinReply(this.me, contacts, this.filetuples, this.agView);
                        sendMessage(reply, peer);
                        break;
                    case INFORM: //DAR O NOVO NO A CONHECER AOS RESTANTES
                        KelipsInformRequest msg = new KelipsInformRequest();
                        sendMessage(msg, peer);
                        break;
                    case INFORM_DONE:
                        //do nothing
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.info("Out Connection to {} is down cause {}", peer, event.getCause());

        BigInteger hash = HashGenerator.generateHash(peer.toString());
        int fromID = hash.mod(BigInteger.valueOf(this.agNum)).intValueExact();
        this.removeContact(fromID, peer);
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} failed cause: {}", peer, event.getCause());

        BigInteger hash = HashGenerator.generateHash(peer.toString());
        int fromID = hash.mod(BigInteger.valueOf(this.agNum)).intValueExact();
        this.removeContact(fromID, peer);
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.debug("In Connection from {} is up", peer);
    }

    //A connection someone established to me is disconnected.
    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.info("{} -> In Connection to {} is down cause {}",me, peer, event.getCause());

        BigInteger hash = HashGenerator.generateHash(peer.toString());
        int fromID = hash.mod(BigInteger.valueOf(this.agNum)).intValueExact();
        logger.info(contacts.toString());
        this.removeContact(fromID, peer);
    }


    /* --------------------------------- Reply ---------------------------- */


    /* --------------------------------- Messages ---------------------------- */
    private void uponInformMessage(KelipsInformRequest msg, Host from, short sourceProto, int channelId) {
        BigInteger hash = HashGenerator.generateHash(from.toString());
        int fromID = hash.mod(BigInteger.valueOf(this.agNum)).intValueExact();
        //Sao do mesmo afinnity group
        if (fromID == myAG) {
            agView.add(from);
        } //Sao de grupos diferentes

        connect(from, Reason.INFORM_DONE);
    }

    private void uponJoinReplyMessage(KelipsJoinReply msg, Host from, short sourceProto, int channelId) {

        Host c = null;
        if (!msg.getContacts().containsKey(this.myAG)) {
            this.agView = msg.getAgView();
            this.filetuples = msg.getFileTuples();
            this.contacts = msg.getContacts();
        } else {
            Set<Host> aux = msg.getContacts().get(this.myAG);
            int index = (int) (Math.random() * aux.size());
            c = (Host) aux.toArray()[index];
        }

        if (c == null) {
            for (Host h : this.agView)
                connect(h, Reason.INFORM);
        }

        if (c != null)
            connect(c, Reason.JOIN);
    }

    private void uponJoinMessage(KelipsJoinRequest msg, Host from, short sourceProto, int channelId) {
        BigInteger hash = HashGenerator.generateHash(from.toString());
        int fromID = hash.mod(BigInteger.valueOf(this.agNum)).intValueExact();

        //Sao do mesmo afinnity group
        if (fromID == myAG) {
            agView.add(from);
        } //Sao de grupos diferentes
        else {
            Set<Host> aux = contacts.get(fromID);
            if (aux == null) {
                aux = new HashSet<Host>();
            }
            aux.add(from);
            contacts.put(fromID, aux);
        }
        connect(from, Reason.JOIN);
    }

    private void uponGetFileMessage(GetFileMessage msg, Host from, short sourceProto, int channelId) {
        Host host = filetuples.get(msg.getObjID()); //may be null
        GetFileReply msgR = new GetFileReply(msg.getObjID(), msg.getUid(), host);
        sendMessage(msgR, from);
    }

    //adapted from lookup
    @SuppressWarnings("DuplicatedCode")
    private void uponGetDiffAgFileMessage(GetDiffAgFileMessage receivedMsg, Host from, short sourceProto, int channelId) {
        Host host;

        if (receivedMsg.getOpType()) {
            host = (Host) agView.toArray()[(int) (Math.random() * agView.size())];
            filetuples.put(receivedMsg.getObjID(), host);

            GetFileReply msgR = new GetFileReply(receivedMsg.getObjID(), receivedMsg.getUid(), host);
            sendMessage(msgR, from);
        } else {

            host = filetuples.get(receivedMsg.getObjID());

            if (host == null) {//I do not know the file
                GetFileMessage msg =
                        new GetFileMessage(receivedMsg.getUid(), receivedMsg.getObjID());

                ongoinglookUp.putIfAbsent(msg.getUid(), agView);

                for (Host h : agView) {
                    sendMessage(msg, h);
                }

            } else {
                GetFileReply msgR = new GetFileReply(receivedMsg.getObjID(), receivedMsg.getUid(), host);
                sendMessage(msgR, from);
            }
        }
    }

    private void uponLookupReplyMessage(GetFileReply msg, Host from, short sourceProto, int channelId) {
        if (ongoinglookUp.containsKey(msg.getUid())) {

            if (msg.getHost() == null) {
                ongoinglookUp.get(msg.getUid()).remove(msg.getHost());

                if (ongoinglookUp.get(msg.getUid()).isEmpty())
                    ongoinglookUp.remove(msg.getUid());

            } else { //if not found - do nothing for now... lets test and see

                List<Host> hlist = new ArrayList<>();
                hlist.add(msg.getHost());
                LookupResponse resp = new LookupResponse(msg.getUid(), msg.getObjID(), hlist);
                sendReply(resp, Storage.PROTOCOL_ID);
                ongoinglookUp.remove(msg.getUid());
            }
        }
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
                             Throwable throwable, int channelId) {

        BigInteger hash = HashGenerator.generateHash(host.toString());
        int fromID = (hash.intValue() % this.agNum);

        removeContact(fromID, host);
    }

    /*--------------------------------- Requests ---------------------------------------- */
    @SuppressWarnings("DuplicatedCode")
    private void uponLookupRequest(LookupRequest lookupRequest, short sourceProto) {
        int fAG = lookupRequest.getObjID().mod(BigInteger.valueOf(agNum)).intValueExact();
        Host host;
        List<Host> hostList = new ArrayList<>();
        logger.info("In Upon Lookup");

        if (fAG == myAG) {
            //opType - True if insert/Put; False if retrieve/Get
            if (lookupRequest.getOpType()) {

                host = (Host) agView.toArray()[(int) (Math.random() * agView.size())];
                hostList.add(host);
                filetuples.put(lookupRequest.getObjID(), host);
                LookupResponse reply =
                        new LookupResponse(lookupRequest.getRequestUID(), lookupRequest.getObjID(), hostList);
                sendReply(reply, Storage.PROTOCOL_ID);

            } else {

                host = filetuples.get(lookupRequest.getObjID());

                if (host == null) {//file on my AG but I do not know it

                    GetFileMessage msg =
                            new GetFileMessage(lookupRequest.getRequestUID(), lookupRequest.getObjID());

                    ongoinglookUp.putIfAbsent(msg.getUid(), agView); //Hugo - same thing as comment inside for, right?

                    for (Host h : agView) { //Hugo - em vez disto porque nao gossip?
                        sendMessage(msg, h);
                        /*
                        Set<Host> aux = ongoinglookUp.get(msg.getUid());
                        if (aux == null)
                            aux = new HashSet<Host>();
                        aux.add(h);
                        ongoinglookUp.put(msg.getUid(), aux);
                         */
                    }

                } else {
                    hostList.add(host);
                    LookupResponse reply =
                            new LookupResponse(lookupRequest.getRequestUID(), lookupRequest.getObjID(), hostList);
                    sendReply(reply, Storage.PROTOCOL_ID);
                }
            }
        } else { // file does not belong my AG

            //opType - True if insert/Put; False if retrieve/Get
            //if (lookupRequest.getOpType()) { /*no need to do nothing*/ }

            logger.info("{} -> contacts {}", me, contacts.toString());
            Set<Host> contact = contacts.get(fAG);
            if (contact != null && contact.size()!=0) {
                int index = (int) (Math.random() * contact.size());
                Host c = (Host) contact.toArray()[index];

                GetDiffAgFileMessage msg =
                        new GetDiffAgFileMessage(lookupRequest.getRequestUID(), lookupRequest.getObjID(),
                                lookupRequest.getOpType());
                sendMessage(msg, c);

                Set<Host> aux = ongoinglookUp.get(msg.getUid());
                if (aux == null)
                    aux = new HashSet<Host>();
                aux.add(c);
                ongoinglookUp.put(msg.getUid(), aux);
            } else {
                logger.info("asneira");
                System.exit(-1);
            }
        }
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
    private void connect(Host peer, Reason reason) {
        logger.info("In connecting");
        if (pending.containsKey(peer)) {
            Set<Reason> set = pending.get(peer);
            set.add(reason);
            pending.replace(peer, set);
        } else {
            Set<Reason> set = new HashSet<>();
            set.add(reason);
            pending.put(peer, set);
        }
        openConnection(peer);
    }

    @SuppressWarnings("DuplicatedCode")
    private void removeContact(int fromID, Host peer) {
        logger.info("In remove Contact");
        if (fromID == myAG) {
            agView.removeIf(n -> n.equals(peer));

            Set<Host> cH = candidates.get(fromID);
            if (cH != null) {
                for (Host h : cH) {
                    if (!agView.contains(h))
                        agView.add(h);
                }
            }
        } else {
            Set<Host> aux = contacts.get(fromID);
            if (aux!=null){//if (!aux.isEmpty())
                aux.removeIf(n -> n.equals(peer));
            }

            Set<Host> cH = candidates.get(fromID);
            if (cH != null) {
                for (Host h : cH) {
                    if (!aux.contains(h))
                        aux.add(h);
                }
            }
            contacts.put(fromID, aux);
        }

        Set<Host> aux = candidates.get(fromID);
        if (aux!=null) {//if (!aux.isEmpty()) {
            if (peer != null && aux.contains(peer))
                aux.removeIf(h -> h.equals(peer));

            candidates.put(fromID, aux);
        }
        pending.remove(peer);

        List<BigInteger> temp = new ArrayList<>();
        filetuples.forEach((i, h) -> {
            if (h.equals(peer)) temp.add(i);
            });
        temp.forEach(i -> filetuples.remove(i));

        closeConnection(peer);
    }

/*
    private void broadcastRequest(GossipTimer timer, long timerId) {
        InformationGossip msg = new InformationGossip(contacts, filetuples, agView);
        BroadcastRequest request = new BroadcastRequest(UUID.randomUUID(), this.me, Serializer.serialize(msg));

        logger.info("Sending: {}  ({})", request.getMsgId(), Serializer.serialize(msg).length);
        //And send it to the dissemination protocol
        //sendRequest(request, FloodBroadcast.PROTOCOL_ID);
        sendRequest(request, ProbReliableBroadcast.PROTOCOL_ID);
    }*/
}
