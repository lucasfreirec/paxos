package com.example;

import java.util.ArrayList;
import java.util.List;

public class Messages {
    public static class Propose { public final Object v; public Propose(Object v) { this.v = v; } }
    public static class Read { public final int ballot; public Read(int ballot) { this.ballot = ballot; } }
    public static class Abort { public final int ballot; public Abort(int ballot) { this.ballot = ballot; } }
    public static class Gather { 
        public final int ballot, estBallot; public final Object est; 
        public Gather(int b, int eb, Object e) { this.ballot = b; this.estBallot = eb; this.est = e; } 
    }
    public static class Impose { 
        public final int ballot; public final Object v; 
        public Impose(int b, Object v) { this.ballot = b; this.v = v; } 
    }
    public static class Ack { public final int ballot; public Ack(int ballot) { this.ballot = ballot; } }
    public static class Decide { public final Object v; public Decide(Object v) { this.v = v; } }
}
