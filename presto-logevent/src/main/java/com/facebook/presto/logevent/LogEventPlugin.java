package com.facebook.presto.logevent;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.google.common.collect.ImmutableSet;

/**
 * Created by xiaosong_niu on 3/9/17.
 */
public class LogEventPlugin
        implements Plugin
{
    public Iterable<EventListenerFactory> getEventListenerFactories()
    {

        return ImmutableSet.<EventListenerFactory>builder()
                .add(new EventListernerFactoryImpl())
                .build();
    }
}
