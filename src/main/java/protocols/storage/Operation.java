package protocols.storage;

import java.math.BigInteger;

public class Operation {

    private static final String GET = "GET";
    private static final String INSERT = "INSERT";

    private boolean opType;
    private BigInteger id;
    private String name;

    public Operation(boolean opType, BigInteger id, String name) {
        this.opType=opType;
        this.id=id;
        this.name=name;
    }

    public String isOpType() {
        return opType?INSERT:GET;
    }

    public BigInteger getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}
