package com.example;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import java.util.*;


public class Main {

    public static int N = 10;
    public static void main(String[] args) throws InterruptedException {

        // Instantiate an actor system
        final ActorSystem system = ActorSystem.create("system");
        final LoggingAdapter log = Logging.getLogger(system, "main");
        
        log.info("System started with N=" + N);

        ArrayList<ActorRef> references = new ArrayList<>();

        for (int i = 0; i < N; i++) {
            // Instantiate processes
            final ActorRef a = system.actorOf(Process.createActor(i + 1, N), "" + i);
            references.add(a);
        }

        //give each process a view of all the other processes
        Members m = new Members(references);
        for (ActorRef actor : references) {
            actor.tell(m, ActorRef.noSender());
        }
        
        OfconsProposerMsg opm = new OfconsProposerMsg("100");
        references.get(0).tell(opm, ActorRef.noSender());
    }
}
