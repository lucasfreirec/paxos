package com.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class Main {

    public static class Monitor extends akka.actor.UntypedAbstractActor {
        private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
        private boolean firstDecisionReceived = false;
        private final CompletableFuture<Long> resultFuture;

        public Monitor(CompletableFuture<Long> resultFuture) {
            this.resultFuture = resultFuture;
        }

        public static akka.actor.Props props(CompletableFuture<Long> resultFuture) {
            return akka.actor.Props.create(Monitor.class, () -> new Monitor(resultFuture));
        }

        @Override
        public void onReceive(Object msg) {
            if (msg instanceof Messages.DecisionTime) {
                Messages.DecisionTime dt = (Messages.DecisionTime) msg;
                if (!firstDecisionReceived) {
                    firstDecisionReceived = true;
                    log.info("FASTEST CONSENSUS: {} decided in {} ms!", dt.processName, dt.latency);
                    this.resultFuture.complete(dt.latency); // Unlock the main thread!
                }
            } else {
                unhandled(msg);
            }
        }
    }

    public static void main(String[] args) {
        // Default values
        int N = 10;
        int F = 3;
        double alpha = 0.1;
        int tle = 500;

        if (args.length >= 4) {
            try {
                N = Integer.parseInt(args[0]);
                F = Integer.parseInt(args[1]);
                alpha = Double.parseDouble(args[2]);
                tle = Integer.parseInt(args[3]);
            } catch (NumberFormatException e) {
                System.err.println("Error parsing arguments. Please provide numbers: <N> <F> <alpha> <tle>");
                System.exit(1);
            }
        } else {
            System.out.println("Usage: java Main <N> <F> <alpha> <tle>");
            System.out.println("Not enough arguments provided. Using defaults...");
        }

        System.out.printf("Starting experiment with: N=%d, F=%d, alpha=%.2f, tle=%d ms%n", N, F, alpha, tle);
        
        long totalLatency = 0;
        int repetitions = 5;

        for (int i = 1; i <= repetitions; i++) {
            try {
                System.out.println("\n--- Starting Trial " + i + " ---");
                long latency = runSingleTrial(N, F, alpha, tle);
                totalLatency += latency;
                System.out.println("Trial " + i + " finished with latency: " + latency + " ms");
            } catch (Exception e) {
                System.err.println("Trial " + i + " failed or timed out.");
                e.printStackTrace();
            }
        }

        long averageLatency = totalLatency / repetitions;
        System.out.println("\n========================================");
        System.out.println("AVERAGE LATENCY (5 runs): " + averageLatency + " ms");
        System.out.println("========================================\n");
        
        System.exit(0); // Force exit to ensure all Akka threads die
    }

    public static long runSingleTrial(int N, int F, double alpha, int tle) throws Exception {
        final ActorSystem system = ActorSystem.create("system");
        final LoggingAdapter log = Logging.getLogger(system, "trial");

        CompletableFuture<Long> latencyFuture = new CompletableFuture<>();
        ActorRef monitor = system.actorOf(Monitor.props(latencyFuture), "monitor");

        ArrayList<ActorRef> references = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            final ActorRef a = system.actorOf(Process.createActor(i + 1, N, alpha), "P" + (i+1));
            references.add(a);
        }

        Members m = new Members(references);
        for (ActorRef actor : references) actor.tell(m, ActorRef.noSender());

        List<ActorRef> shuffledRefs = new ArrayList<>(references);
        Collections.shuffle(shuffledRefs);
        List<ActorRef> faultProneProcesses = shuffledRefs.subList(0, F);
        
        for (ActorRef fp : faultProneProcesses) {
            fp.tell(new Messages.Crash(), ActorRef.noSender());
        }

        long startTime = System.currentTimeMillis();
        for (ActorRef actor : references) {
            actor.tell(new Messages.Launch(startTime, monitor), ActorRef.noSender());
        }

        system.scheduler().scheduleOnce(
            Duration.create(tle, TimeUnit.MILLISECONDS),
            () -> {
                List<ActorRef> safeProcesses = new ArrayList<>(references);
                safeProcesses.removeAll(faultProneProcesses);
                
                if (!safeProcesses.isEmpty()) {
                    int randomIndex = new java.util.Random().nextInt(safeProcesses.size());
                    ActorRef leader = safeProcesses.get(randomIndex);
                    log.info("Elected Leader: {}", leader.path().name());
                    
                    for (ActorRef actor : references) {
                        if (!actor.equals(leader)) {
                            actor.tell(new Messages.Hold(), ActorRef.noSender());
                        } else {
                            actor.tell(new Messages.Launch(startTime, monitor), ActorRef.noSender());
                        }
                    }
                }
            },
            system.dispatcher()
        );

        long finalLatency = latencyFuture.get();

        system.terminate();
        Thread.sleep(1000); // Give Akka a moment to release ports

        return finalLatency;
    }
}