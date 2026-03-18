package com.example;

import akka.actor.UntypedAbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import akka.event.Logging;
import akka.event.LoggingAdapter;

public class Process extends UntypedAbstractActor {

    private LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    static public Props props(String name) {
        return Props.create(Process.class, () -> new Process(name));
    }

    private Members members;

    private final String name;

    public Process(String name) {
        this.name = name;
    }

    @Override
    public void onReceive(Object msg) throws Exception {
        if (msg instanceof Members) {
            this.members = (Members) msg;
            for (int x = 0; x < this.members.num; x = x + 1) {
                log.info(this.name + ": know member " + Integer.toString(x));
            }
        } else if (msg instanceof String) {
            ActorRef actorRef = getSender();
            log.info(this.name + ": received new message '" + msg + "' from " + actorRef);
        }
    }

    public static Props createActor(int i, int n) {
        return Props.create(Process.class, () -> {
            return new Process("P" + Integer.toString(i));
        });
    }
}
