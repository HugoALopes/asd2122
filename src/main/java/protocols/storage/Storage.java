package protocols.storage;

import membership.common.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.replies.LookupResponse;
import protocols.dht.requests.LookupRequest;
import protocols.storage.messages.*;
import protocols.storage.replies.*;
import protocols.storage.requests.*;
import protocols.timers.CacheDeleteTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.*;

import static utils.HashGenerator.generateHash;

public class Storage extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(Storage.class);

    // Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "Store";
    public static final short PROTOCOL_ID = 200;
    public static final short DHT_PROTOCOL = 1100;
    public static final short APP_PROTOCOL = 300;

    private static final int CACHE_TIMEOUT = 50000;
    private final Host me;
    //@SuppressWarnings("FieldCanBeLocal")
    //private int channelId;

    private final Map<BigInteger, CacheContent> cache;
    private final Map<BigInteger, byte[]> store;
    private final Map<UUID, Operation> context;

    private Set<Host> connections;
    
    private boolean channelReady;

    public Storage(Properties props, Host myself) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        me = myself;
        cache = new HashMap<>();
        store = new HashMap<>();
        context = new HashMap<>();
        connections = new HashSet<>();
        //contextId=0;

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(StoreRequest.REQUEST_ID, this::uponStoreRequest);
        registerRequestHandler(RetrieveRequest.REQUEST_ID, this::uponRetrieveRequest);

        /*--------------------- Register Reply Handlers ----------------------------- */
        registerReplyHandler(LookupResponse.REPLY_ID, this::uponLookupResponse);

        /*--------------------- Register Notification Handlers ----------------------------- */
        // subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        // subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(CacheDeleteTimer.TIMER_ID, this::uponCacheDeleteTimer);
    }

    @Override
    public void init(Properties props) {

        // setup timer to delete cache
        //setupPeriodicTimer(new CacheDeleteTimer(), CACHE_TIMEOUT, CACHE_TIMEOUT);
    }

    // Upon receiving the channelId from the membership, register our own callbacks
    // and serializers
    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
        int cId = notification.getChannelId();
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);
        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, SaveMessage.MSG_ID, SaveMessage.serializer);
        registerMessageSerializer(cId, SuccessSaveMessage.MSG_ID, SuccessSaveMessage.serializer);
        registerMessageSerializer(cId, GetMessage.MSG_ID, GetMessage.serializer);
        registerMessageSerializer(cId, ThereYouGoMessage.MSG_ID, ThereYouGoMessage.serializer);
        registerMessageSerializer(cId, NoFileInHostMessage.MSG_ID, NoFileInHostMessage.serializer);
        /*---------------------- Register TCP Handlers -------------------------- */

        try {
            registerMessageHandler(cId, SaveMessage.MSG_ID, this::uponSaveMessage, this::uponSaveMsgFail);
            registerMessageHandler(cId, SuccessSaveMessage.MSG_ID, this::uponSuccessSaveMessage, this::uponFailSave);
            registerMessageHandler(cId, GetMessage.MSG_ID, this::uponGetMessage, this::uponFailGet);
            registerMessageHandler(cId, ThereYouGoMessage.MSG_ID, this::uponThereYouGoMessage, this::uponFailRetrieve);
            registerMessageHandler(cId, NoFileInHostMessage.MSG_ID, this::uponNoFileInHost, this::uponSaveMsgFail);
        } catch (HandlerRegistrationException e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        // Now we can start sending messages
        channelReady = true;
    }

    private void findHost(Operation op) {

        UUID cont = UUID.randomUUID();
        context.put(cont, op);
        // find "saver" host
        LookupRequest getHost = new LookupRequest(op.getId(), cont, op.getOpType());
        sendRequest(getHost, DHT_PROTOCOL);
    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponStoreRequest(StoreRequest request, short sourceProto) {

        if (!channelReady){
            logger.info("The channel isn't ready to use.");
            return;
        }

        BigInteger id = generateHash(request.getName());
        if(id.signum() == -1){
        	id = id.negate();
        }
        byte[] content = request.getContent();
        cache.put(id, new CacheContent(LocalDateTime.now(), content)); //Porque é que é necessário colocar em cache?

        findHost(new Operation(true, id, request.getName()));
    }

    private void uponRetrieveRequest(RetrieveRequest request, short sourceProto) {
        if (!channelReady){
            logger.info("The channel isn't ready to use.");
            return;
        }

        BigInteger id = generateHash(request.getName());
        if(id.signum() == -1){
        	id = id.negate();
        }
           
        byte[] content = (cache.get(id) == null) ? store.get(id) : cache.get(id).getContent();
	

        if (content == null) {
            findHost(new Operation(false, id, request.getName()));
        } else {
            // TODO - uuid pode ser random? - provavelmente sim
            sendReply(new RetrieveOKReply(request.getName(), null, content), APP_PROTOCOL);
        }
    }

    /*--------------------------------- Replies ---------------------------------------- */
    @SuppressWarnings("UnnecessaryLocalVariable")
    private void uponLookupResponse(LookupResponse response, short sourceProto) {
        UUID contID = response.getMid(); //TODO - remove when uuid in response done
        List<Host> hostList = response.getHost();
        context.get(contID).setHostList(hostList);

        if (context.get(contID).getOpType()) { //True if insert/Put; False if retrieve/Get
            byte[] content = cache.get(response.getObjId()).getContent(); // se isto demorar o objeto pode ja não estar em cache
            //byte[] content = (cache.get(response.getObjId()) == null) ? store.get(response.getObjId()) : cache.get(response.getObjId()).getContent();


            hostList.forEach(host -> {
                if (host.equals(me)) {
                    store.put(response.getObjId(), content);
                    sendReply(new StoreOKReply(context.get(contID).getName(), contID), APP_PROTOCOL);
                }
                else{
                	SaveMessage requestMsg = new SaveMessage(contID, response.getObjId(), host, content);
                	openConnection(host);
                	sendMessage(requestMsg, host);
                }
            });

        } else {
            Operation op = context.get(contID);
            Host host = op.getHost();
	
	    if(host.equals(me)){
	    	if(op.nextHost())
	    		host = op.getHost();
	    }
	     
	    GetMessage getMsg = new GetMessage(contID, context.get(contID).getId());
            openConnection(host);
            sendMessage(getMsg, host);
	 	
            
        }
    }

    /*--------------------------------- Messages ---------------------------------------- */
    private void uponNoFileInHost(NoFileInHostMessage msg, Host host, short proto, int channelId){
        Operation op = context.get(msg.getUid());
        if(!op.alreadyAskedAll()){
            Host h = null;
            
            if(host.equals(me)){
	    	if(op.nextHost())
	    		host = op.getHost();
	    }
	      
            GetMessage getMsg = new GetMessage(msg.getUid(), context.get(msg.getUid()).getId());
            openConnection(h);
            sendMessage(getMsg, h);
        } else {
            sendReply(new RetrieveFailedReply(context.get(msg.getUid()).getName(), msg.getUid()), APP_PROTOCOL);
            context.remove(msg.getUid());
            logger.info("Não consegui encontrar um nó com o ficheiro procurado");
        }
        
    }

    private void uponSaveMessage(SaveMessage msg, Host host, short proto, int channelId) {
        store.put(msg.getObjId(), msg.getContent());
        sendMessage(new SuccessSaveMessage(msg.getMid(), null), host);
    }

    private void uponSuccessSaveMessage(SuccessSaveMessage msg, Host host, short proto, int channelId) {
        sendReply(new StoreOKReply(context.get(msg.getUid()).getName(), msg.getUid()), APP_PROTOCOL);
        context.remove(msg.getUid());
    }

    private void uponGetMessage(GetMessage getMsg, Host host, short proto, int channelId) {
        byte[] content = store.get(getMsg.getObjId());
        if(content != null){
            sendMessage(new ThereYouGoMessage(getMsg.getMid(), getMsg.getObjId(), host, store.get(getMsg.getObjId())),
                host);
        } else {
            sendMessage(new NoFileInHostMessage(getMsg.getMid()), host);
        }   
    }

    private void uponThereYouGoMessage(ThereYouGoMessage msg, Host host, short proto, int channelId) {
        sendReply(new RetrieveOKReply(null, msg.getMid(), msg.getContent()), APP_PROTOCOL);
    }

    private void uponFailGet(GetMessage msg, Host host, short proto, Throwable throwable, int channelId) {
        // TODO - check host + check if clause
       /* if (context.get(msg.getMid()).nextHost() && cache.get(context.get(msg.getMid()).getId()) != null) {
            GetMessage getMsg = new GetMessage(msg.getMid(), msg.getObjId());
            sendMessage(getMsg, host);
        } else {
            sendReply(new RetrieveFailedReply(context.get(msg.getMid()).getName(), msg.getMid()), APP_PROTOCOL);
            context.remove(msg.getMid());
            logger.error("Object {} retrieval failed", msg.getObjId());
        }*/

        Operation op = context.get(msg.getMid());
        if(!op.alreadyAskedAll()){
            Host h = op.getHost(); 
            GetMessage getMsg = new GetMessage(msg.getMid(), context.get(msg.getMid()).getId());
            openConnection(h);
            sendMessage(getMsg, h);
        } else {
            sendReply(new RetrieveFailedReply(context.get(msg.getMid()).getName(), msg.getMid()), APP_PROTOCOL);
            context.remove(msg.getMid());
            logger.info("Não consegui encontrar um nó com o ficheiro procurado");
        }
    }

    private void uponFailRetrieve(ThereYouGoMessage msg, Host host, short destProto, Throwable throwable,
                                  int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
        sendReply(new RetrieveFailedReply(context.get(msg.getMid()).getName(), msg.getMid()), APP_PROTOCOL);
    }

    private void uponSaveMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        // If a message fails to be sent, for whatever reason, log the message and the
        // reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponFailSave(SuccessSaveMessage msg, Host host, short proto, Throwable throwable, int channelId) {
        logger.error("Retrieve of {} failed, reason: {}", msg.getId(), throwable);
    }

    /*--------------------------------- Timers ---------------------------------------- */
    private void uponCacheDeleteTimer(CacheDeleteTimer timer, long timerId) {
        LocalDateTime present = LocalDateTime.now();
        ArrayList<BigInteger> aux = new ArrayList<>();
        cache.forEach((id, obj) -> {
            if (obj.getTime().isBefore(present)) {
                aux.add(id);
                logger.debug("Item {} removed from cache", id);
            }
        });

        for (BigInteger key : aux) {
            cache.remove(key);
        }
    }

    /*--------------------------------- TCP ---------------------------------------- */
    // If a connection is successfully established, this event is triggered.

}