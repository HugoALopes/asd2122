package protocols.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.replies.LookupResponse;
import protocols.dht.requests.LookupRequest;
import protocols.storage.messages.SaveRequestMsg;
import protocols.storage.replies.StoreOKReply;
import protocols.storage.requests.RetrieveRequest;
import protocols.storage.requests.StoreRequest;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
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
    //TODO - change ME from null to right thing
    private final static Host ME = null;

    private Map<BigInteger,Object> cache;
    private Map<BigInteger,Object> store;


    public Storage(String protoName, short protoId) {
        super(protoName, protoId);

        cache = new HashMap<>();
        store = new HashMap<>();
<<<<<<< HEAD

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
=======
>>>>>>> parent of cf998ca... storage big fixes + small start kelips + timers
    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {

    }

<<<<<<< HEAD
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
=======
    private void uponStoreRequest(StoreRequest request, short sourceProto){
>>>>>>> parent of cf998ca... storage big fixes + small start kelips + timers

        BigInteger id = generateHash(request.getName());
        byte[] content = request.getContent();
        cache.put(id,content);

        Host host = null;
        //TODO - timer to delete cache

        //find "saver" host
        LookupRequest getHost = new LookupRequest(id);
        sendRequest(getHost, DHT_PROTOCOL );

        //uponLookupResponse...
        if (host.equals(ME)) {
            store.put(id, content);
        } else {
            //send msg to host to store
            sendMessage(null,host);
        }

        //make sense? reply UID = request.getRequestUID
        StoreOKReply storeOk = new StoreOKReply(request.getName(), request.getRequestUID());
        sendReply(storeOk, sourceProto);
    }

    private void uponRetrieveRequest(RetrieveRequest request, short sourceProto) {
        BigInteger id = generateHash(request.getName());

        Object content = cache.get(id);

        if (content.equals(null))
            content = store.get(id);

        if (content.equals(null)) {
            //TODO - not sure
            LookupRequest getHost = new LookupRequest(id);
            sendRequest(getHost, DHT_PROTOCOL);

        }
    }

<<<<<<< HEAD
    /*--------------------------------- Replies ---------------------------------------- */
    //TODO - check if right -> how to pass content
    private void uponLookupResponse(LookupResponse response, short sourceProto) {
        Host host = response.getHost();
        byte[] content= cache.get(response.getObjId());
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
        //TODO - delete cache
    }
=======
    //TODO - check if right
    private void uponLookupResponse (LookupResponse response, byte[] content, short sourceProto) {

        SaveRequestMsg requestMsg = new SaveRequestMsg(response.getObjId(), response.getHost(), content);
        sendMessage(requestMsg, requestMsg.getHost());
    }

    private void uponSaveMsgResponse (){

    }
        //RetrieveOkReply
        //RetrieveOKReply replyOk = new RetrieveOKReply(request.getName(), request.getRequestUID(),null);
        //sendMessage(replyOk,null);

>>>>>>> parent of cf998ca... storage big fixes + small start kelips + timers
}
