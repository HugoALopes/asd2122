package protocols.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.requests.LookupRequest;
import protocols.storage.replies.RetrieveOKReply;
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
    public static final short PROTOCOL_ID = 100;

    private Map<BigInteger,Object> cache;
    private Map<BigInteger,Object> store;

    //TODO - change ME from null to right thing
    private final static Host ME = null;

    public Storage(String protoName, short protoId) {
        super(protoName, protoId);

        cache = new HashMap<>();
        store = new HashMap<>();
    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {

    }

    private void uponStoreRequest(StoreRequest request, String name, byte[] content){
        BigInteger id = generateHash(name);
        cache.put(id,content);

        Host host = null;
        //TODO
        /**
         * timer to delete cache
         * pedir host to save at DHT
         */

        if (host.equals(ME)) {
            cache.put(id, content);
            store.put(id, content);
        } else {
            //send msg to host to store
            StoreRequest req = new StoreRequest(null, content);
            sendMessage(null,null);
        }

        sendMessage(null,null);//storeRequestOk
    }

    private void uponRetrieveRequest(String name){
        BigInteger id = generateHash(name);

        Object content = cache.get(id);

        if (content.equals(null))
            content = store.get(id);

        if (content.equals(null)) {
            //TODO
            /**
             * ask for host of file
             */
            //from request
            LookupRequest getHost = new LookupRequest(id);
            //sendMessage(getHost,null);

            //RetrieveOkReply
            RetrieveOKReply replyOk = new RetrieveOKReply(null,null,null);
            //sendMessage(replyOk,null);
        }
    }
}
