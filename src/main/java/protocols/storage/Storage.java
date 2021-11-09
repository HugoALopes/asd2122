package protocols.storage;

import channel.notifications.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.replies.LookupResponse;
import protocols.dht.requests.LookupRequest;
import protocols.storage.messages.SaveMessage;
import protocols.storage.messages.SuccessSaveMessage;
import protocols.storage.replies.RetrieveFailedReply;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
    private final Map<Integer, Operation> context;
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
            registerMessageHandler(cId, SaveMessage.MSG_ID, this::uponSaveMessage, this::uponMsgFail);
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
    private int nextContext(){
        return (contextId==Integer.MAX_VALUE)?0:contextId++;
    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponStoreRequest(StoreRequest request, short sourceProto) {

        BigInteger id = generateHash(request.getName());
        byte[] content = request.getContent();
        cache.put(id, new CacheContent(LocalDateTime.now(),content));

        context.put(nextContext(),new Operation(true, id, request.getName()));

        //find "saver" host
        LookupRequest getHost = new LookupRequest(id);
        sendRequest(getHost, DHT_PROTOCOL);
    }

    private void uponRetrieveRequest(RetrieveRequest request, short sourceProto) {
        BigInteger id = generateHash(request.getName());

        byte[] content = (cache.get(id) == null)?store.get(id):cache.get(id).getContent();

        if (content == null) {
            context.put(nextContext(),new Operation(false, id, request.getName()));

            LookupRequest getHost = new LookupRequest(id);
            sendRequest(getHost, DHT_PROTOCOL);
        } else
            sendReply(new StoreOKReply(request.getName(),null),APP_PROTOCOL);
    }

    /*--------------------------------- Replies ---------------------------------------- */
    //TODO - change for host[]
    private void uponLookupResponse(LookupResponse response, short sourceProto) {
        Host host = response.getHost();
        byte[] content= cache.get(response.getObjId()).getContent();
        if (host.equals(me)) {
            store.put(response.getObjId(), content);
        } else {
            //send msg to host to store
            SaveMessage requestMsg = new SaveMessage(null,response.getObjId(),
                    response.getHost(), content);
            sendMessage(requestMsg, host);
        }
    }

    /*--------------------------------- Messages ---------------------------------------- */
    private void uponSaveMessage(SaveMessage msg, Host host, short proto, int channelId) {
        store.put(msg.getObjId(), msg.getContent());
        sendMessage(new SuccessSaveMessage(msg.getMid(),null),host);
    }

    private void uponSuccessSaveMessage(SuccessSaveMessage msg, Host host, short proto, int channelId) {
        sendReply(new StoreOKReply(msg.getName(), msg.getUid()),APP_PROTOCOL);
    }
    
    private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
                             Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponFailSave(SuccessSaveMessage msg, Host host, short proto, Throwable throwable, int channelId) {
        logger.error("Retrieve of {} failed, reason: {}", msg.getId(), throwable);
        sendReply(new RetrieveFailedReply(msg.getName(),msg.getUid()),APP_PROTOCOL);
    }

    /*--------------------------------- Timers ---------------------------------------- */
    private void uponCacheDeleteTimer(CacheDeleteTimer timer, long timerId) {
        LocalDateTime present = LocalDateTime.now();
        cache.forEach((id,obj) -> {
            if (obj.getTime().isBefore(present)) {
                cache.remove(id);
                logger.info("Removed from cache {}", id);
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
