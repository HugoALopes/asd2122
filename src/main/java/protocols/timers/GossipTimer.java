package protocols.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class GossipTimer extends ProtoTimer {
    public static final short TIMER_ID = 502;

    public GossipTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
