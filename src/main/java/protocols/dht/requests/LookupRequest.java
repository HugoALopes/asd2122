package protocols.dht.requests;

import java.math.BigInteger;
import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class LookupRequest extends ProtoRequest {

	public final static short REQUEST_ID = 110;
	
	private BigInteger id;
	private UUID uid;
	private Boolean opType;

	public LookupRequest(BigInteger id, UUID uid, Boolean opType) {
		super(REQUEST_ID);
		this.id = id;
		this.uid = uid;
		this.opType=opType;
	}
	
	public UUID getRequestUID() {
		return this.uid;
	}
	
	public BigInteger getObjID() {
		return this.id;
	}

	public Boolean getOpType() {
		return opType;
	}
}
