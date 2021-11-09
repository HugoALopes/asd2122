package protocols.dht;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.requests.LookupRequest;
import protocols.timers.*;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.channel.tcp.events.ChannelMetrics;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

import static utils.HashGenerator.generateHash;

public class Kelips extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(Kelips.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "Kelips";

    private final Host me;
    private Random rnd;

    private final Set<Host> agView;
    private final Map<BigInteger, Host[]> contacts;
    private final Map<BigInteger, Host> filetuples;
    private int agNum = 4;
    private final int myAG;

    private final int channelId;

    public Kelips(Host self, int agNum) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.me = self;
        rnd = new Random();

        channelId = 0;

        filetuples = new HashMap<>();
        contacts = new HashMap<>();
        agView = new HashSet<>();
        this.agNum = agNum;
        myAG = generateHash(me.toString()).intValue() % this.agNum; //TODO - check this


        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(LookupRequest.REQUEST_ID, this::uponLookupRequest);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTime);
    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {
        //TODO - do stuff
    }

    /*--------------------- Notifications subscribed ----------------------------- */
    private void uponDeliver(DeliverNotification not, short sourceProto){
        Host sender = not.getSender();

        
        BigInteger hash = HashGenerator.generateHash(sender.toString());
        int fromID = (hash.intValue() % this.agNum);

        InformationGossip ig = Serializer.deserialize(not.getMsg());
        candidates = ig.getContacts();

        Set<Host> aux = candidates.get(fromID);
        if(aux == null){
            aux = new HashSet<Host>();
            aux = ig.getAgView();
            candidates.put(fromID, aux);
        }

        for(Integer key: ig.getFileTuples().keySet()){
            if(!filetuples.containsKey(key))
                filetuples.put(key, ig.getFileTuples().get(key));

        }
    }


    /*--------------------------------- TCP ---------------------------------------- */
    // If a connection is successfully established, this event is triggered.
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        triggerNotification(new NeighbourDown(peer));
        Set<Reason> reasons = pending.remove(peer);
        logger.debug("Out Connection to {} is up.", peer);
        if (reasons != null) {
            for(Reason reason: reasons){
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
        logger.debug("Out Connection to {} is down cause {}", peer, event.getCause());
        
        BigInteger hash = HashGenerator.generateHash(peer.toString());
        int fromID = hash.mod(BigInteger.valueOf(this.agNum)).intValueExact();
        this.removeContact(fromID, peer);
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} failed cause: {}", peer, event.getCause());

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
        logger.debug("In Connection to {} is down cause {}", peer, event.getCause());

        BigInteger hash = HashGenerator.generateHash(peer.toString());
        int fromID = hash.mod(BigInteger.valueOf(this.agNum)).intValueExact();
        this.removeContact(fromID, peer);
    }


    /* --------------------------------- Reply ---------------------------- */
    

    /* --------------------------------- Messages ---------------------------- */
    private void uponInformMessage(KelipsInformRequest msg, Host from, short sourceProto, int channelId) {
        BigInteger hash = HashGenerator.generateHash(from.toString());
        int fromID = hash.mod(BigInteger.valueOf(this.agNum)).intValueExact();
        //Sao do mesmo afinnity group
        if(fromID == myAG){
            agView.add(from);
        } //Sao de grupos diferentes

        connect(from, Reason.INFORM_DONE);
    }

    private void uponJoinReplyMessage(KelipsJoinReply msg, Host from, short sourceProto, int channelId) {

        Host c = null;
        if(!msg.getContacts().containsKey(this.myAG)){
            this.agView = msg.getAgView();
            this.filetuples = msg.getFileTuples();
            this.contacts = msg.getContacts();
        }else{
            Set<Host> aux = msg.getContacts().get(this.myAG);
            int index = (int)(Math.random() * aux.size());
            c = (Host) aux.toArray()[index];
        }

        if(c == null){
            for(Host h: this.agView)
                connect(h, Reason.INFORM);
        }
        
        if(c != null)
            connect(c, Reason.JOIN);
    }

    private void uponJoinMessage(KelipsJoinRequest msg, Host from, short sourceProto, int channelId) {
        BigInteger hash = HashGenerator.generateHash(from.toString());
        int fromID = hash.mod(BigInteger.valueOf(this.agNum)).intValueExact();
        
        //Sao do mesmo afinnity group
        if(fromID == myAG){
            agView.add(from);
        } //Sao de grupos diferentes
        else{
            Set<Host> aux = contacts.get(fromID);
            if(aux == null){
                aux = new HashSet<Host>();
                aux.add(from);
            }
            else if(aux.size() < this.agNum){
                aux.add(from);
            }
            contacts.put(fromID, aux);
        }   
        connect(from, Reason.JOIN);
    }

    private void uponGetFileRequest(GetFileRequest msg, Host from, short sourceProto, int channelId){
        Host host = filetuples.get(msg.getObjID().intValue());
        GetFileReply msgR = new GetFileReply(msg.getObjID(), msg.getUid());
        sendMessage(msgR, host);
    }

    private void uponLookupReplyMessage(GetFileReply msg, Host from, short sourceProto, int channelId){
        if(ongoinglookUp.containsKey(msg.getUid())){
            LookupResponse resp = new LookupResponse(msg.getObjID(), from);
            sendReply(resp, Storage.PROTOCOL_ID);
            ongoinglookUp.remove(msg.getUid());
        }
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
        Throwable throwable, int channelId){

        BigInteger hash = HashGenerator.generateHash(host.toString());
        int fromID = (hash.intValue() % this.agNum);
    
        removeContact(fromID, host);
    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponLookupRequest(LookupRequest lookupRequest, short sourceProto) {
        //TODO - check bigInteger to int
        int fAG = lookupRequest.getObjID().intValue() % agNum;
        Host host;
        if (fAG == myAG) {
            host = filetuples.get(lookupRequest.getObjID());

            if (host == null){
                //TODO - host <- gossip ( fAG, (GETHOST, id) )
            }

        } else { //file does not belong my AG
            Host contact = contacts.get(fAG)[rnd.nextInt(contacts.get(fAG).length)];

            //TODO - send msg
            sendMessage(null, contact);
        }

    }

    /* --------------------------------- Metrics ---------------------------- */

    //If we setup the InfoTimer in the constructor, this event will be triggered periodically.
    //We are simply printing some information to present during runtime.
    private void uponInfoTime(InfoTimer timer, long timerId) {
        //TODO - log stuff
        /*StringBuilder sb = new StringBuilder("Membership Metrics:\n");
        sb.append("Membership: ").append(membership).append("\n");
        sb.append("PendingMembership: ").append(pending).append("\n");
        //getMetrics returns an object with the number of events of each type processed by this protocol.
        //It may or may not be useful to you, but at least you know it exists.
        sb.append(getMetrics());
        logger.info(sb);
         */
    }

    //If we passed a value >0 in the METRICS_INTERVAL_KEY property of the channel, this event will be triggered
    //periodically by the channel. This is NOT a protocol timer, but a channel event.
    //Again, we are just showing some of the information you can get from the channel, and use how you see fit.
    //"getInConnections" and "getOutConnections" returns the currently established connection to/from me.
    //"getOldInConnections" and "getOldOutConnections" returns connections that have already been closed.
    private void uponChannelMetrics(ChannelMetrics event, int channelId) {
        StringBuilder sb = new StringBuilder("Channel Metrics:\n");
        sb.append("In channels:\n");
        event.getInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.append("Out channels:\n");
        event.getOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.setLength(sb.length() - 1);
        logger.info(sb);
    }

    /* --------------------------------- Utils ---------------------------- */
    private void connect(Host peer, Reason reason) {
        if(pending.containsKey(peer)){
            Set<Reason> set = pending.get(peer);
            set.add(reason);
            pending.replace(peer, set);
        }else{
            Set<Reason> set = new HashSet<>();
            set.add(reason);
            pending.put(peer, set);
        }
        openConnection(peer);
    }

    private void removeContact(int fromID, Host peer){
        if(fromID == myAG){
            for(Host n: agView){
                if(n.equals(peer))
                    agView.remove(n);
            }
            Set<Host> cH = candidates.get(fromID);
            if(cH != null){
                for(Host h: cH){
                    if(!agView.contains(h))
                        agView.add(h);
                }
            }
        }else{
            Set<Host> aux = contacts.get(fromID);
            for(Host n: aux){
                if(n.equals(peer))
                    aux.remove(n); 
            }
            Set<Host> cH = candidates.get(fromID);
            if(cH != null){
                for(Host h: cH){
                    if(!aux.contains(h))
                        aux.add(h);
                }
            }
            contacts.put(fromID, aux);
        }  

        Set<Host> aux = candidates.get(fromID);
        for(Host h: aux){
            if(h.equals(peer))
                aux.remove(h);
        }
        candidates.put(fromID, aux);
        pending.remove(peer);
        filetuples.forEach((i,h) -> {
            if(h.equals(peer))filetuples.remove(i);
        });
        /*
        for(Host h: filetuples.values()){
            //Remover o host dos filetuples se houver
        }
        */
        closeConnection(peer);
    }

    private void broadcastRequest(GossipTimer timer, long timerId) {
        InformationGossip msg = new InformationGossip(contacts, filetuples, agView);
        BroadcastRequest request = new BroadcastRequest(UUID.randomUUID(), this.me, Serializer.serialize(msg));
        
        logger.info("Sending: {}  ({})", request.getMsgId(), Serializer.serialize(msg).length);
        //And send it to the dissemination protocol
        sendRequest(request, FloodBroadcast.PROTOCOL_ID);
    }

}
