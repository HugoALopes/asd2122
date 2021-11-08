package protocols.storage;

import channel.notifications.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.replies.LookupResponse;
import protocols.dht.requests.LookupRequest;
import protocols.storage.messages.SaveMessage;
import protocols.storage.requests.RetrieveRequest;
import protocols.storage.requests.StoreRequest;
import protocols.timers.*;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDate;
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

    private static final int CACHE_TIMEOUT = 50000;
    private final Host me;

    private final Map<BigInteger, CacheContent> cache;
    private final Map<BigInteger, byte[]> store;

    private boolean channelReady;

    public Storage(Properties properties, Host myself) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        me = myself;
        cache = new HashMap<>();
        store = new HashMap<>();

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

    //TODO - check!! -> copied from example
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
        } catch (HandlerRegistrationException e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        //Now we can start sending messages
        channelReady = true;
    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponStoreRequest(StoreRequest request, short sourceProto) {

        BigInteger id = generateHash(request.getName());
        byte[] content = request.getContent();
        cache.put(id, new CacheContent(LocalDateTime.now(),content));

        //find "saver" host
        LookupRequest getHost = new LookupRequest(id);
        sendRequest(getHost, DHT_PROTOCOL);
    }

    private void uponRetrieveRequest(RetrieveRequest request, short sourceProto) {
        BigInteger id = generateHash(request.getName());

        byte[] content = ((cache.get(id) == null)?store.get(id):cache.get(id).getContent());

        if (content == null) {
            //TODO - not sure
            LookupRequest getHost = new LookupRequest(id);
            sendRequest(getHost, DHT_PROTOCOL);
        }
    }

    /*--------------------------------- Replies ---------------------------------------- */
    //TODO - check if right
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
    private void uponSaveMessage(SaveMessage msg, Host host, short i, int i1) {
        store.put(msg.getObjId(), msg.getContent());
    }
    
    private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
                             Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
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
}
