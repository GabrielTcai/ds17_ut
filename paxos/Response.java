package paxos;
import java.io.Serializable;

/**
 * Please fill in the data structure you use to represent the response message for each RMI call.
 * Hint: You may need a boolean variable to indicate ack of acceptors and also you may need proposal number and value.
 * Hint: Make it more generic such that you can use it for each RMI call.
 */
public class Response implements Serializable {
    static final long serialVersionUID=2L;
    // your data here
    boolean stat;
    int n;
    int n_a;
    Object v_a;

    // Your constructor and methods here
    Response(boolean stat, int n, int n_a, Object v_a){
        this.stat = stat;
        this.n = n;
        this.n_a = n_a;
        this.v_a = v_a;
    }

    Response(boolean stat){
        this.stat = stat;
        this.n = 0;
        this.n_a = 0;
        this.v_a = null;
    }
}
