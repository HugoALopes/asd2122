package protocols.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class BroadcastTimer extends ProtoTimer {
    public static final short TIMER_ID = 500;

    public BroadcastTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}