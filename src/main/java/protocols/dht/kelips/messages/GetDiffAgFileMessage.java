package protocols.dht.kelips.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

import java.math.BigInteger;
import java.util.UUID;

public class GetDiffAgFileMessage extends ProtoMessage{
    public final static short MESSAGE_ID = 135;

	private UUID uid;
    private BigInteger objID;
    private boolean opType;


    public GetDiffAgFileMessage(UUID mid, BigInteger objID, boolean opType) {
        super(MESSAGE_ID);
        this.uid = mid;
        this.objID = objID;
        this.opType=opType;
    }

    public BigInteger getObjID(){
        return this.objID;
    }

    public UUID getUid(){
        return this.uid;
    }

    public boolean getOpType() {
        return opType;
    }
}
