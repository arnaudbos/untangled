package io.monkeypatch.untangled.experiments;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class Experiment02_Generator {

    public static void main(String[] args) {
        System.out.println("main");

        //<editor-fold desc="state machine iterator">
        Iterator<String> iter = new StateMachineIterator();
        while(iter.hasNext()) {
            System.out.println(iter.next());
        }
        //</editor-fold>

        //<editor-fold desc="continuation based iterator">
        iter = new ContinuationIterator();
        while(iter.hasNext()) {
            System.out.println(iter.next());
        }
        //</editor-fold>
    }
}

class StateMachineIterator implements Iterator<String> {
    private int state;
    private int i;
    public String next() {
        switch(state) {
            case 0: state=1; return "A";
            case 1: state=2; i=0; return "B";
            case 2:
                if(i == 3) state = 3;
                return "C" + (i++);
            case 3: state=4; return "D";
            case 4: state=5; return "E";
            default: throw new NoSuchElementException();
        }
    }
    public boolean hasNext() {
        return state < 5;
    }
    public void remove() {
        throw new UnsupportedOperationException("Not supported");
    }
}

class ContinuationIterator implements Iterator<String> {
    private ContinuationScope scope = new ContinuationScope("SCOPE");

    String element = null;

    Continuation c = new Continuation(scope, () -> {
        element = "A";
        Continuation.yield(scope);

        element = "B";
        Continuation.yield(scope);

        for(int i = 0; i < 4; i++) {
            element = "C" + i;
            Continuation.yield(scope);
        }

        element = "D";
        Continuation.yield(scope);

        element = "E";
    });

    @Override
    public boolean hasNext() {
        if (element!=null) {
            return true;
        }
        if (c.isDone()) {
            return false;
        }
        c.run();
        return element!=null;
    }

    @Override
    public String next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        var element = this.element;
        this.element = null;
        return element;
    }
}