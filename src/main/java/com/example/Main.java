package com.example;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import java.util.*;
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;


public class Main {

    public static int N = 10;
    public static int F = 3;

    // Add this inside the Main class:
    public static class Monitor extends akka.actor.UntypedAbstractActor {
        private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
        private boolean firstDecisionReceived = false;

        public static akka.actor.Props props() {
            return akka.actor.Props.create(Monitor.class, Monitor::new);
        }

        @Override
        public void onReceive(Object msg) {
            if (msg instanceof Messages.DecisionTime) {
                Messages.DecisionTime dt = (Messages.DecisionTime) msg;
                
                // The first one to arrive is the shortest time!
                if (!firstDecisionReceived) {
                    firstDecisionReceived = true;
                    log.info("🏆 FASTEST CONSENSUS: {} decided in {} ms!", dt.processName, dt.latency);
                }
                //  else {
                //     // Optional: Log the runner-ups
                //     log.info("   Runner up: {} decided in {} ms.", dt.processName, dt.latency);
                // }
            } else {
                unhandled(msg);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {

        // Instantiate an actor system
        final ActorSystem system = ActorSystem.create("system");
        final LoggingAdapter log = Logging.getLogger(system, "main");
        
        log.info("System started with N=" + N);

        ArrayList<ActorRef> references = new ArrayList<>();

        for (int i = 0; i < N; i++) {
            // Instantiate processes
            final ActorRef a = system.actorOf(Process.createActor(i + 1, N), "P" + Integer.toString(i+1));
            references.add(a);
        }

        //give each process a view of all the other processes
        Members m = new Members(references);
        for (ActorRef actor : references) {
            actor.tell(m, ActorRef.noSender());
        }
        // references.get(0).tell(new Messages.Propose("100"), ActorRef.noSender());

        List<ActorRef> shuffledRefs = new ArrayList<>(references);
        Collections.shuffle(shuffledRefs);
        List<ActorRef> faultProneProcesses = shuffledRefs.subList(0, F);
        
        for (ActorRef fp : faultProneProcesses) {
            fp.tell(new Messages.Crash(), ActorRef.noSender());
        }

        ActorRef monitor = system.actorOf(Monitor.props(), "monitor");

        long startTime = System.currentTimeMillis();
        log.info("Starting consensus protocol at timestamp: {}", startTime);

        for (ActorRef actor : references) {
            actor.tell(new Messages.Launch(startTime, monitor), ActorRef.noSender());
        }

        system.scheduler().scheduleOnce(
            Duration.create(500, TimeUnit.MILLISECONDS),
            () -> {
                log.info("--- Executing Leader Election ---");
                
                // Find a process that is NOT fault-prone
                List<ActorRef> safeProcesses = new ArrayList<>(references);
                safeProcesses.removeAll(faultProneProcesses);
                
                if (!safeProcesses.isEmpty()) {
                    ActorRef leader = safeProcesses.get(0);
                    log.info("Elected Leader: {}", leader.path().name());
                    
                    // Send 'Hold' to every process EXCEPT the elected leader
                    for (ActorRef actor : references) {
                        if (!actor.equals(leader)) {
                            actor.tell(new Messages.Hold(), ActorRef.noSender());
                        }
                    }
                }
            },
            system.dispatcher()
        );

    }
}
