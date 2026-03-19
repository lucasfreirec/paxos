package com.example;

import akka.actor.UntypedAbstractActor;

import java.util.HashMap;
import java.util.Map;

import akka.actor.ActorRef;
import akka.actor.Props;

import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.util.Random;

public class Process extends UntypedAbstractActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    
    // --- System & Topology ---
    private Members members;
    private final String name;
    private final int i; // Process index (1 to N)
    private final int n; // Total processes (N)
    private final int randomVal;

    // --- Failure Injection State ---
    private final double alpha = 0.1; // 10% chance to crash on any event if fault-prone
    private boolean isSilent = false; // True if the process has crashed
    private boolean isFaultProne = false; // True if designated to potentially crash
    private boolean holdProposing = false; // True if instructed to stop proposing (Leader Election)

    // --- Synod Algorithm State ---
    private int ballot;
    private Object proposal = null;
    private int readballot = 0;
    private int imposeballot;
    private Object estimate = null;
    private int ackCount = 0;
    private boolean decided = false;
    
    // Tracks Gather responses
    private static class StateEntry {
        int estBallot; Object est;
        StateEntry(int eb, Object e) { this.estBallot = eb; this.est = e; }
    }
    private Map<ActorRef, StateEntry> states = new HashMap<>();

    private long launchTime;
    private ActorRef monitor = null;

    // --- Constructor & Factory ---
    public Process(String name, int i, int n) {
        this.name = name;
        this.i = i;
        this.n = n;
        // Ballot initialized to i - n to ensure unique ballots per process
        this.ballot = i - n; 
        this.imposeballot = i - n;
        this.randomVal = new Random().nextInt(2); // Randomly pick 0 or 1
    }

    public static Props createActor(int i, int n) {
        return Props.create(Process.class, () -> new Process("P" + i, i, n));
    }

    // --- Message Handling ---
    @Override
    public void onReceive(Object msg) {
        // Check if process is dead
        if (isSilent) {
            log.debug("{} is silent and ignoring message.", name);
            return; 
        }

        // Fault-prone crash check on every event
        if (isFaultProne && Math.random() < alpha) {
            log.error("{} CRASHED while processing {}! Entering silent mode.", name, msg.getClass().getSimpleName());
            isSilent = true;
            return;
        }

        // Message Routing
        if (msg instanceof Members) {
            this.members = (Members) msg;
            log.info("{}: updated members list (Size: {}).", name, members.num);
        } 
        else if (msg instanceof Messages.Crash) {
            this.isFaultProne = true;
            log.warning("{} entered FAULT-PRONE mode.", name);
        } 
        else if (msg instanceof Messages.Hold) {
            this.holdProposing = true;
            log.info("{} received HOLD. Will stop proposing.", name);
        } 
        else if (msg instanceof Messages.Read) {
            handleRead((Messages.Read) msg);
        } 
        else if (msg instanceof Messages.Gather) {
            handleGather((Messages.Gather) msg);
        } 
        else if (msg instanceof Messages.Impose) {
            handleImpose((Messages.Impose) msg);
        } 
        else if (msg instanceof Messages.Ack) {
            handleAck((Messages.Ack) msg);
        } 
        else if (msg instanceof Messages.Decide) {
            handleDecide((Messages.Decide) msg);
        } 
        else if (msg instanceof Messages.Abort) {
            log.warning("{} received ABORT for ballot {}.", name, ((Messages.Abort) msg).ballot);
            
            Messages.Abort abortMsg = (Messages.Abort) msg;
            if (abortMsg.ballot != ballot) {
                return; // ignore aborts from old attempts
            }
            // propose again
            handleLaunch();
        } 
        else if (msg instanceof String) {
            log.info("{}: received string message '{}' from {}", name, msg, getSender());
        } 
        else if (msg instanceof Messages.Launch) {
            this.launchTime = ((Messages.Launch) msg).startTime;
            this.monitor = ((Messages.Launch) msg).monitor; // Save the monitor
            handleLaunch();
        }
        else {
            unhandled(msg);
        }
    }

    // --- Algorithm Implementations ---

    private void handleLaunch() {
        if (!holdProposing && !decided && !isSilent) {
            log.info("{} launching PROPOSE operation with value: {}", name, randomVal);
            propose();
        }
    }

    private void propose() {
        proposal = this.randomVal;
        ballot += n;
        ackCount = 0; 
        states.clear();
        broadcast(new Messages.Read(ballot));
    }

    private void handleRead(Messages.Read msg) {
        if (readballot > msg.ballot || imposeballot > msg.ballot) {
            getSender().tell(new Messages.Abort(msg.ballot), getSelf());
        } else {
            readballot = msg.ballot;
            getSender().tell(new Messages.Gather(msg.ballot, imposeballot, estimate), getSelf());
        }
    }

    private void handleGather(Messages.Gather msg) {

        // ignore stale/future messages
        if (msg.ballot != ballot) {
            return;
        }
        states.put(getSender(), new StateEntry(msg.estBallot, msg.est));

        // Upon receiving a majority of responses
        if (states.size() > n / 2) {
            StateEntry highest = null;
            for (StateEntry s : states.values()) {
                if (s.estBallot > 0) {
                    if (highest == null || s.estBallot > highest.estBallot) {
                        highest = s;
                    }
                }
            }
            if (highest != null) {
                proposal = highest.est; // Adopt the value with the highest ballot
            }
            states.clear(); // Clear so we don't trigger this multiple times for the same ballot
            broadcast(new Messages.Impose(ballot, proposal));
        }
    }

    private void handleImpose(Messages.Impose msg) {
        if (readballot > msg.ballot || imposeballot > msg.ballot) {
            getSender().tell(new Messages.Abort(msg.ballot), getSelf());
        } else {
            estimate = msg.v;
            imposeballot = msg.ballot;
            getSender().tell(new Messages.Ack(msg.ballot), getSelf());
        }
    }

    private void handleAck(Messages.Ack msg) {

        if (msg.ballot != ballot) {
            return;
        }

        ackCount++;
        // Upon receiving a majority of ACKs
        if (ackCount > n / 2 && !decided) {
            broadcast(new Messages.Decide(proposal));
        }
    }

    private void handleDecide(Messages.Decide msg) {
        if (!decided) {
            decided = true;
            
            long latency = System.currentTimeMillis() - this.launchTime;
            log.info(">>>> Process {} DECIDED on value: {} in {} ms <<<<", name, msg.v, latency);
            if (this.monitor != null) {
                this.monitor.tell(new Messages.DecisionTime(latency, name), getSelf());
            }
            
            broadcast(new Messages.Decide(msg.v)); // Reliable broadcast to peers
        }
    }

    // --- Helpers ---
    
    private void broadcast(Object msg) {
        if (this.members == null || this.members.getActorRefs() == null) {
            log.error("Members not initialized for broadcast!");
            return;
        }
        for (ActorRef actor : this.members.getActorRefs()) {
            actor.tell(msg, getSelf());
        }
    }
}