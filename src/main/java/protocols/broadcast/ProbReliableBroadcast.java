package protocols.broadcast;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.EmptyByteBuf;
import membership.common.ChannelCreated;
import membership.common.NeighbourDown;
import membership.common.NeighbourUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;
import protocols.broadcast.messages.FloodMessage;
import protocols.dht.kelips.Kelips;
import protocols.dht.kelips.messages.InformationGossip;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.Serializer;

import java.io.IOException;
import java.util.*;

public class ProbReliableBroadcast extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(ProbReliableBroadcast.class);

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "ProbReliableBroadcast";
    public static final short PROTOCOL_ID = 400;

    private final Host myself; //My own address/port
    private Set<Host> neighbours; //My known neighbours (a.k.a peers the membership protocol told me about)
    private Host[] neighboursAUXList;
    private final Set<UUID> received; //Set of received messages (since we do not want to deliver the same msg twice)
    private int fanout;

    //We can only start sending messages after the membership protocol informed us that the channel is ready
    private boolean channelReady;

    public ProbReliableBroadcast(Properties properties, Host myself) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;
        neighbours = new HashSet<>();
        received = new HashSet<>();
        channelReady = false;
        fanout = 0;
        //neighboursAUXList = new ArrayList<>();


        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        //subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        //subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
    }

    @Override
    public void init(Properties props) {
        //Nothing to do here, we just wait for event from the membership or the application
    }

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
        int cId = notification.getChannelId();
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);
        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, FloodMessage.MSG_ID, FloodMessage.serializer);
        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(cId, FloodMessage.MSG_ID, this::uponFloodMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        //Now we can start sending messages
        channelReady = true;
    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
        if (!channelReady) return;
        neighbours = request.getGroup();
        neighboursAUXList = (Host [])neighbours.toArray();
        //Create the message object.
        FloodMessage msg = new FloodMessage(request.getMsgId(), request.getSender(), sourceProto, request.getMsg());

        //Call the same handler as when receiving a new FloodMessage (since the logic is the same)
        uponFloodMessage(msg, myself, getProtoId(), -1);
    }

    /*--------------------------------- Messages ---------------------------------------- */
    private void uponFloodMessage(FloodMessage msg, Host from, short sourceProto, int channelId) {
        logger.trace("Received {} from {}", msg, from);
        //If we already received it once, do nothing (or we would end up with a nasty infinite loop)
        if (received.add(msg.getMid())) {
            //Deliver the message to the application (even if it came from it)
            //triggerNotification(new DeliverNotification(msg.getMid(), msg.getSender(), msg.getContent()));
            ByteBuf buf = new EmptyByteBuf(ByteBufAllocator.DEFAULT);
            buf.writeBytes(msg.getContent());
            sendReply(Serializer.deserialize(buf), Kelips.PROTOCOL_ID);
            fanout=(int)Math.log(neighbours.size());
            Random rnd = new Random();
            Set <Integer> index = new HashSet<>();
            while (index.size()<fanout)
                index.add(rnd.nextInt());
            index.forEach(i -> {
                Host h = neighboursAUXList[i];
                if (!h.equals(from)) {
                    logger.trace("Sent {} to {}", msg, h);
                    sendMessage(msg, h);
                }
            });
        }
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
                             Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /*--------------------------------- Notifications ---------------------------------------- */

    //When the membership protocol notifies of a new neighbour (or leaving one) simply update my list of neighbours.
    /*private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        for(Host h: notification.getNeighbours()) {
            if(neighbours.add(h))
                neighboursAUXList.add(h);
            logger.info("New neighbour: " + h);
        }
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        for(Host h: notification.getNeighbours()) {
            if (neighbours.remove(h))
                neighboursAUXList.remove(h);
            logger.info("Neighbour down: " + h);
        }
    }*/
}
