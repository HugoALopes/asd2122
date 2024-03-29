package protocols.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class CacheDeleteTimer extends ProtoTimer {

    public static final short TIMER_ID = 501;

    public CacheDeleteTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
