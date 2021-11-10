package protocols.storage;

import channel.notifications.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.replies.LookupResponse;
import protocols.dht.requests.LookupRequest;
import protocols.storage.messages.SaveMessage;
import protocols.storage.messages.SuccessSaveMessage;
import protocols.storage.replies.RetrieveFailedReply;
import protocols.storage.replies.RetrieveOKReply;
import protocols.storage.replies.StoreOKReply;
import protocols.storage.requests.RetrieveRequest;
import protocols.storage.requests.StoreRequest;
import protocols.timers.CacheDeleteTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.*;

import static utils.HashGenerator.generateHash;

public class Storage extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(Storage.class);

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "Store";
    public static final short PROTOCOL_ID = 200;
    public static final short DHT_PROTOCOL = 100;
    public static final short APP_PROTOCOL = 300;

    private static final int CACHE_TIMEOUT = 50000;
    private final Host me;
    private int channelId;

    private final Map<BigInteger, CacheContent> cache;
    private final Map<BigInteger, byte[]> store;
    private final Map<UUID, Operation> context;
    private int contextId;

    private boolean channelReady;

    public Storage(Properties props, Host myself) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        me = myself;
        cache = new HashMap<>();
        store = new HashMap<>();
        context = new HashMap<>();
        contextId=0;

        channelId = 0;

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

        /*--------------------- TCPEvents ----------------------------- */
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(StoreRequest.REQUEST_ID, this::uponStoreRequest);
        registerRequestHandler(RetrieveRequest.REQUEST_ID, this::uponRetrieveRequest);

        /*--------------------- Register Reply Handlers ----------------------------- */
        registerReplyHandler(LookupRequest.REQUEST_ID, this::uponLookupResponse);

        /*--------------------- Register Notification Handlers ----------------------------- */
        //subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        //subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(CacheDeleteTimer.TIMER_ID, this::uponCacheDeleteTimer);
    }

    @Override
    public void init(Properties properties) {

        //setup timer to delete cache
        setupPeriodicTimer(new CacheDeleteTimer(), CACHE_TIMEOUT, CACHE_TIMEOUT);
    }

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
        int cId = notification.getChannelId();
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);
        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, SaveMessage.MSG_ID, SaveMessage.serializer);
        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(cId, SaveMessage.MSG_ID, this::uponSaveMessage, this::uponSaveMsgFail);
            registerMessageHandler(cId, SuccessSaveMessage.MSG_ID, this::uponSuccessSaveMessage, this::uponFailSave);
        } catch (HandlerRegistrationException e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        //Now we can start sending messages
        channelReady = true;
    }

    /*--------------------------------- AUX ---------------------------------------- */
    private int nextContext(){ return (contextId==Integer.MAX_VALUE)?0:contextId++; }

    private void findHost(Operation op){

        UUID cont = UUID.randomUUID();
        context.put(cont, op);//new Operation(true, id, request.getName()) );
        //find "saver" host
        LookupRequest getHost = new LookupRequest( op.getId(), cont);
        sendRequest(getHost, DHT_PROTOCOL);
    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponStoreRequest(StoreRequest request, short sourceProto) {
        if (!channelReady) return;

        BigInteger id = generateHash(request.getName());
        byte[] content = request.getContent();
        cache.put(id, new CacheContent(LocalDateTime.now(),content));

        findHost(new Operation(true, id, request.getName()));
    }

    private void uponRetrieveRequest(RetrieveRequest request, short sourceProto) {
        if (!channelReady) return;

        BigInteger id = generateHash(request.getName());
        byte[] content = (cache.get(id) == null)?store.get(id):cache.get(id).getContent();

        if (content == null) {
            findHost(new Operation(false, id, request.getName()));
        } else {
            //TODO - uuid pode ser random? - provavelmente sim
            sendReply(new RetrieveOKReply(request.getName(), null, content) , APP_PROTOCOL);
        }
    }

    /*--------------------------------- Replies ---------------------------------------- */
    //TODO - change for host[] -> change get(0) - context inside msg's
    private void uponLookupResponse(LookupResponse response, short sourceProto) {
        UUID responseUID = UUID.randomUUID(); //TODO - remove when uuid in response done
        UUID contID = responseUID;
        List<Host> hostList = response.getHost();
        context.get(contID).setHostList(hostList);

        if (context.get(contID).getOpType()){ //True if insert/Put; False if retrieve/Get
            byte[] content= cache.get(response.getObjId()).getContent();

            hostList.forEach(host -> {
                if (host.equals(me)) {
                    store.put(response.getObjId(), content);
                    sendReply(new StoreOKReply(context.get(contID).getName(), contID),APP_PROTOCOL);
                }
            });

            hostList.forEach(host -> {
                SaveMessage requestMsg = new SaveMessage(contID,response.getObjId(), host, content);
                sendMessage(requestMsg, host);
            });

        } else {
            Host host = context.get(contID).getHost();
            //SaveMessage requestMsg = new SaveMessage(contID,response.getObjId(), host, content);
            //sendMessage(requestMsg, host);
        }
    }

    /*--------------------------------- Messages ---------------------------------------- */
    private void uponSaveMessage(SaveMessage msg, Host host, short proto, int channelId) {
        store.put(msg.getObjId(), msg.getContent());
        sendMessage(new SuccessSaveMessage(msg.getMid(),null),host);
    }

    private void uponSuccessSaveMessage(SuccessSaveMessage msg, Host host, short proto, int channelId) {
        sendReply(new StoreOKReply(context.get(msg.getUid()).getName(), msg.getUid()),APP_PROTOCOL);
        context.remove(msg.getUid());
    }
    
    private void uponSaveMsgFail(ProtoMessage msg, Host host, short destProto,
                                 Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    //TODO - check host + check if clause
    private void uponFailSave(SuccessSaveMessage msg, Host host, short proto, Throwable throwable, int channelId) {
        logger.error("Retrieve of {} failed, reason: {}", msg.getId(), throwable);

        if(context.get(msg.getUid()).nextHost() && cache.get(context.get(msg.getUid()).getId()) != null ){
            SaveMessage requestMsg = new SaveMessage(msg.getUid(),context.get(msg.getUid()).getId(), host,
                    cache.get(context.get(msg.getUid()).getId()).getContent());
            sendMessage(requestMsg, context.get(msg.getUid()).getHost());
        } else {
            sendReply(new RetrieveFailedReply(msg.getName(), msg.getUid()), APP_PROTOCOL);
            context.remove(msg.getUid());
        }
    }

    /*--------------------------------- Timers ---------------------------------------- */
    private void uponCacheDeleteTimer(CacheDeleteTimer timer, long timerId) {
        LocalDateTime present = LocalDateTime.now();
        cache.forEach((id,obj) -> {
            if (obj.getTime().isBefore(present)) {
                cache.remove(id);
                logger.debug("Item {} removed from cache", id);
            }
        });
    }

    /*--------------------------------- TCP ---------------------------------------- */
    // If a connection is successfully established, this event is triggered.
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Out Connection to {} is up.", peer);
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Out Connection to {} is down cause {}", peer, event.getCause());
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} failed cause: {}", peer, event.getCause());
    }


    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.debug("In Connection from {} is up", peer);
    }

    //A connection someone established to me is disconnected.
    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.debug("In Connection to {} is down cause {}", peer, event.getCause());
    }
}
