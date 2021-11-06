package protocols.dht;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.messages.GetHostMessage;
import protocols.dht.messages.KelipsInformReply;
import protocols.dht.messages.KelipsInformRequest;
import protocols.dht.messages.KelipsJoinRequest;
import protocols.dht.messages.KelipsJoinReply;
import protocols.dht.replies.LookupResponse;
import protocols.dht.requests.LookupRequest;
import protocols.timers.*;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.channel.tcp.events.ChannelMetrics;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HashGenerator;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.net.InetAddress;

import static utils.HashGenerator.generateHash;

public class Kelips extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(Kelips.class);

    //enum para os tempos de razao para a conexao estar pendente
    public enum Reason {
        NEW_JOIN, JOIN, INFORM, INFORM_DONE
    }

    // Protocol information, to register in babel
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "Kelips";
    private final int channelId;


    //numero de contactos 
    private int agNum = 4;

    //informaçao do proprio
    private final Host me;
    private final int myAG;
    private Random rnd;

    //Conexoes pendentes ainda nao estabelecidas
    private final Map<Host, Reason> pending;

    //Soft state do no
    private Set<Host> agView;
    private Map<Integer, ArrayList<Host>> contacts;
    private Map<Integer, Host> filetuples;
    
 

    public Kelips(Host self, int agNum) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        
        this.me = self;
        rnd = new Random();
        this.agNum = agNum;
        myAG = generateHash(me.toString()).intValue() % this.agNum; // TODO - check this
        
        channelId = 0;

        pending = new HashMap<>();

        filetuples = new HashMap<>();
        contacts = new HashMap<>();
        agView = new HashSet<>();
       
    

        /*--------------------- Register Message Handlers ----------------------------- */
        registerMessageHandler(channelId, KelipsJoinRequest.REQUEST_ID, this::uponJoinMessage, this::uponMsgFail);
        registerMessageHandler(channelId, KelipsJoinReply.REQUEST_ID, this::uponJoinReplyMessage, this::uponMsgFail);
        registerMessageHandler(channelId, KelipsInformRequest.REQUEST_ID, this::uponInformMessage, this::uponMsgFail);
        registerMessageHandler(channelId, KelipsInformReply.REQUEST_ID, this::uponInformReplyMessage, this::uponMsgFail);


        /*--------------------- Register Reply Handlers ----------------------------- */
        registerReplyHandler(LookupResponse.REPLY_ID, this::uponLookupReplyMessage);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(LookupRequest.REQUEST_ID, this::uponLookupRequest);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTime);

        /*--------------------- TCPEvents ----------------------------- */
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {
        if (properties.contains("contact")) {
            try {
                String contact = properties.getProperty("contact");
                String[] hostElems = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                connect(contactHost, Reason.NEW_JOIN);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + properties.getProperty("contact"));
                System.exit(-1);
            }
        }

    }

    /*--------------------------------- TCP ---------------------------------------- */
    // If a connection is successfully established, this event is triggered.
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        Reason reason = pending.remove(peer);
        logger.debug("Out Connection to {} is up.", peer);
        if (reason != null) {
            switch (reason) {
            case NEW_JOIN: // SEND JOIN REQUEST DE UM NOVO NO
                sendMessage(new KelipsJoinRequest(this.me), peer);
                break;
            case JOIN: //QUANDO UM NO RECEBE UM JOIN REQUEST ENVIA A SUA VIEW E A SI MESMO
                KelipsJoinReply reply = new KelipsJoinReply(agView, this.me);
                sendMessage(reply, peer);
                break;
            case INFORM: //DAR O NOVO NO A CONHECER AOS RESTANTES
                KelipsInformRequest msg = new KelipsInformRequest();
                sendMessage(msg, peer);
                break;
            case INFORM_DONE:
                KelipsInformReply replyI = new KelipsInformReply();
                sendMessage(replyI, peer);
                break;
            default:
                break;
            }

        }
    }

    /* --------------------------------- Reply ---------------------------- */
    private void uponLookupReplyMessage(LookupResponse msg, short sourceProto){

    }

    /* --------------------------------- Messages ---------------------------- */
    private void uponInformReplyMessage(KelipsInformRequest msg, Host from, short sourceProto, int channelId){

    }

    private void uponInformMessage(KelipsInformRequest msg, Host from, short sourceProto, int channelId) {
        BigInteger hash = HashGenerator.generateHash(from.toString());
        int fromID = (hash.intValue() % this.agNum);
        //Sao do mesmo afinnity group
        if(fromID == myAG){
            agView.add(from);
        } //Sao de grupos diferentes
        else{
            ArrayList<Host> aux = contacts.get(fromID);
            if(aux == null){
                aux = new ArrayList<Host>();
                aux.add(from);
            }
            else if(aux.size() < this.agNum){
                aux.add(from);
            }
            contacts.put(fromID, aux);
        }   
        connect(from, Reason.INFORM_DONE);
    }

    private void uponJoinReplyMessage(KelipsJoinReply msg, Host from, short sourceProto, int channelId) {
        BigInteger hash = HashGenerator.generateHash(from.toString());
        int fromID = (hash.intValue() % this.agNum);

        if(fromID == myAG) {
            agView = msg.getAgView();
            for(Host h: agView)
                connect(h, Reason.INFORM);
        }else{
            ArrayList<Host> aux = contacts.get(fromID);
            if(aux == null){
                aux = new ArrayList<Host>();
                aux.add(from);
            }
            else if(aux.size() < this.agNum){
                aux.add(from);
            }

            Iterator<Host> it = msg.getAgView().iterator();
            while(aux.size() < this.agNum && it.hasNext())
                aux.add(it.next());

            contacts.put(fromID, aux);
            for(Host h: aux)
                connect(h, Reason.INFORM);
        }


    }

    private void uponJoinMessage(KelipsJoinRequest msg, Host from, short sourceProto, int channelId) {
        BigInteger hash = HashGenerator.generateHash(from.toString());
        int fromID = (hash.intValue() % this.agNum);
        //Sao do mesmo afinnity group
        if(fromID == myAG){
            agView.add(from);
        } //Sao de grupos diferentes
        else{
            ArrayList<Host> aux = contacts.get(fromID);
            if(aux == null){
                aux = new ArrayList<Host>();
                aux.add(from);
            }
            else if(aux.size() < this.agNum){
                aux.add(from);
            }
            contacts.put(fromID, aux);
        }   
        connect(from, Reason.JOIN);
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
        Throwable throwable, int channelId){

    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponLookupRequest(LookupRequest lookupRequest, short sourceProto) {
        // TODO - check bigInteger to int
        int fAG = lookupRequest.getObjID().intValue() % agNum;
        Host host;
        if (fAG == myAG) {
            host = filetuples.get(lookupRequest.getObjID());

            if (host == null) {
                // TODO - host <- gossip ( fAG, (GETHOST, id) )
            }

        } else { // file does not belong my AG
            //Host contact = contacts.get(fAG)[rnd.nextInt(contacts.get(fAG).length)];

            // TODO - send msg - GETHOST
            GetHostMessage ghMsg = new GetHostMessage(null, lookupRequest.getObjID());
            //sendMessage(ghMsg, contact);
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
        pending.put(peer, reason);
        openConnection(peer);
    }

}
