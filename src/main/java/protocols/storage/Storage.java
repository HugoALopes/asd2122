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
    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {

    }

    private void uponStoreRequest(StoreRequest request, short sourceProto){

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

}
