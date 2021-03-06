package paxos;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is the main class you need to implement paxos instances.
 */
public class Paxos implements PaxosRMI, Runnable{

    ReentrantLock mutex;
    String[] peers; // hostname
    int[] ports; // host port
    int me; // index into peers[]
    //static int proNum = 0;
    public static long startTime;
    static {
        startTime = System.nanoTime(); 
    }

    Registry registry;
    PaxosRMI stub;

    AtomicBoolean dead;// for testing
    AtomicBoolean unreliable;// for testing

    // Your data here
    Map<Integer, Instance> allInst;
    int seqId;
    Object seqVal;
    final int MAJORITY;
    int[] dones;
    Queue<Arg> q;

    class Arg{
        int seq;
        Object val;
        Arg(int seq, Object val){
            this.seq = seq;
            this.val = val;
        }
    }

    class Instance{
        long n_p;
        long n_a;
        Object v_a;
        retStatus status;

        Instance(){
            this.n_p = Integer.MIN_VALUE;
            this.n_a = Integer.MIN_VALUE;
            this.v_a = null;
            this.status = new retStatus(State.Pending, null);
        }
    } 

    /**
     * Call the constructor to create a Paxos peer.
     * The hostnames of all the Paxos peers (including this one)
     * are in peers[]. The ports are in ports[].
     */
    public Paxos(int me, String[] peers, int[] ports){

        this.me = me;
        this.peers = peers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.dead = new AtomicBoolean(false);
        this.unreliable = new AtomicBoolean(false);

        // Your initialization code here
        MAJORITY = peers.length/2 + 1;
        allInst = new HashMap<Integer, Instance>();
        seqId = -1;
        seqVal = null;
        dones = new int[ports.length];
        for(int i = 0; i < ports.length; i++){
            dones[i] = -1;
        }
        q = new LinkedList<Arg>();

        // register peers, do not modify this part
        try{
            System.setProperty("java.rmi.server.hostname", this.peers[this.me]);
            registry = LocateRegistry.createRegistry(this.ports[this.me]);
            stub = (PaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("Paxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }


    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a response message if
     * the server responded, and return null if Call() was not
     * be able to contact the server.
     *
     * You should assume that Call() will time out and return
     * null after a while if it doesn't get a reply from the server.
     *
     * Please use Call() to send all RMIs and please don't change
     * this function.
     */
    public Response Call(String rmi, Request req, int id){
        Response callReply = null;

        PaxosRMI stub;
        try{
            Registry registry=LocateRegistry.getRegistry(this.ports[id]);
            stub=(PaxosRMI) registry.lookup("Paxos");
            if(rmi.equals("Prepare"))
                callReply = stub.Prepare(req);
            else if(rmi.equals("Accept"))
                callReply = stub.Accept(req);
            else if(rmi.equals("Decide"))
                callReply = stub.Decide(req);
            else
                System.out.println("Wrong parameters!");
        } catch(Exception e){
            return null;
        }
        return callReply;
    }


    /**
     * The application wants Paxos to start agreement on instance seq,
     * with proposed value v. Start() should start a new thread to run
     * Paxos on instance seq. Multiple instances can be run concurrently.
     *
     * Hint: You may start a thread using the runnable interface of
     * Paxos object. One Paxos object may have multiple instances, each
     * instance corresponds to one proposed value/command. Java does not
     * support passing arguments to a thread, so you may reset seq and v
     * in Paxos object before starting a new thread. There is one issue
     * that variable may change before the new thread actually reads it.
     * Test won't fail in this case.
     *
     * Start() just starts a new thread to initialize the agreement.
     * The application will call Status() to find out if/when agreement
     * is reached.
     */
    public void Start(int seq, Object value){
        // Your code here
        //System.out.println(me + "start seq: " + seq);
        if(seq < Min()){
            return;
        } 
        mutex.lock();
        //System.out.println(me + "start seq: " + seq);
        //seqId = seq;
        //seqVal = value;
        q.offer(new Arg(seq, value));
        mutex.unlock();
        Thread t = new Thread(this);
        t.start();
    }

    @Override
    public void run(){
        //Your code here
        int mySeqId;
        Object mySeqVal;
        mutex.lock();
        Arg currArg = q.poll();
        mutex.unlock();
        mySeqId = currArg.seq;
        mySeqVal = currArg.val;
        //System.out.println(me + "run seq: " + mySeqId);

        while(true){
            long currProposeNum = genProNum(mySeqId);
            //System.out.println(me + " propose num: " + currProposeNum);
            Object toSendVal = sendPrepare(mySeqId, currProposeNum, mySeqVal);
            boolean status = (toSendVal != null);
            //System.out.println(me + " returned Val: " + status + " for " + currProposeNum);
            if(status){
                status = sendAccept(mySeqId, currProposeNum, toSendVal);
                //System.out.println(me + " Accepted: " + status + " for " + currProposeNum);
                if(status){
                    sendDecide(mySeqId, currProposeNum, toSendVal);
                    break;
                }
            }
            if(Status(mySeqId) != null && Status(mySeqId).state == State.Decided) break;
        }
    }

    private Object sendPrepare(int mySeqId, long currProposeNum, Object myVal){
        long n_a = Integer.MIN_VALUE;
        Object v_a = myVal;
        int okNum = 0;
        
        for(int i = 0; i < ports.length; i++){
            Response currResp;
            if(i == me){
                currResp = Prepare(new Request(mySeqId, currProposeNum, null));
            } else {
                currResp = Call("Prepare", new Request(mySeqId, currProposeNum, null), i);
            }

            if(currResp != null && currResp.stat == true && currResp.n == currProposeNum){
                okNum++;
                if(currResp.v_a != null && currResp.n_a > n_a){
                    n_a = currResp.n_a;
                    v_a = currResp.v_a;
                }
            } 
        }

        if(okNum >= MAJORITY){
            return v_a;
        } else {
            return null;
        }
    }

    private long genProNum(int mySeqId){
        return (System.nanoTime() - startTime);
        //mutex.lock();
        //int num = proNum++;
        //mutex.unlock();
        //return num;
    }

    private boolean sendAccept(int mySeqId, long currProposeNum, Object toSendVal){
        int acceptNum = 0;
        for(int i = 0; i < ports.length; i++){
            Response currResp;
            if(i == me){
                currResp = Accept(new Request(mySeqId, currProposeNum, toSendVal));
            } else{
                currResp = Call("Accept", new Request(mySeqId, currProposeNum, toSendVal), i);
            }

            if(currResp != null && currResp.stat == true) acceptNum++;
        }

        if(acceptNum >= MAJORITY){
            return true;
        } else {
            return false;
        }
    }

    private void sendDecide(int mySeqId, long currProposeNum, Object toSendVal){
        for(int i = 0; i < ports.length; i++){
            //Response currResp;
            if(i == me){
                Decide(new Request(mySeqId, currProposeNum, me, dones[me], toSendVal));
            } else {
                Call("Decide", new Request(mySeqId, currProposeNum, me, dones[me], toSendVal), i);
            }
        }

    }


    // RMI handler
    public Response Prepare(Request req){
        // your code here
        mutex.lock();
        if(!allInst.containsKey(req.instId)){
            allInst.put(req.instId, new Instance());
        }
        Instance currInst = allInst.get(req.instId);

        if(req.n > currInst.n_p){
            currInst.n_p = req.n;
            mutex.unlock();
            return new Response(true, req.n, currInst.n_a, currInst.v_a);
        } else {
            mutex.unlock();
            return new Response(false);
        }

    }

    public Response Accept(Request req){
        // your code here
        mutex.lock();
        if(!allInst.containsKey(req.instId)){
            allInst.put(req.instId, new Instance());
        }
        Instance currInst = allInst.get(req.instId);

        if(req.n >= currInst.n_p){
            currInst.n_p = req.n;
            currInst.n_a = req.n;
            currInst.v_a = req.v;
            mutex.unlock();
            return new Response(true);
        } else {
            mutex.unlock();
            return new Response(false);
        }

    }

    public Response Decide(Request req){
        // your code here
        mutex.lock();
        if(!allInst.containsKey(req.instId)){
            allInst.put(req.instId, new Instance());
        }
        Instance currInst = allInst.get(req.instId);
        currInst.status.state = State.Decided;
        currInst.status.v = req.v;
        dones[req.portId] = req.maxDone;
        //System.out.println(me + " Decided for " + req.instId);

        mutex.unlock();
        return new Response(true);
    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     *
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        // Your code here
        mutex.lock();
        if(seq > dones[me]){
            dones[me] = seq;
        }
        mutex.unlock();
    }


    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max(){
        // Your code here
        int max = -1;
        for(Integer key : allInst.keySet()){
            if(key > max) max = key;
        }
        return max;
    }

    /**
     * Min() should return one more than the minimum among z_i,
     * where z_i is the highest number ever passed
     * to Done() on peer i. A peers z_i is -1 if it has
     * never called Done().

     * Paxos is required to have forgotten all information
     * about any instances it knows that are < Min().
     * The point is to free up memory in long-running
     * Paxos-based servers.

     * Paxos peers need to exchange their highest Done()
     * arguments in order to implement Min(). These
     * exchanges can be piggybacked on ordinary Paxos
     * agreement protocol messages, so it is OK if one
     * peers Min does not reflect another Peers Done()
     * until after the next instance is agreed to.

     * The fact that Min() is defined as a minimum over
     * all Paxos peers means that Min() cannot increase until
     * all peers have been heard from. So if a peer is dead
     * or unreachable, other peers Min()s will not increase
     * even if all reachable peers call Done. The reason for
     * this is that when the unreachable peer comes back to
     * life, it will need to catch up on instances that it
     * missed -- the other peers therefore cannot forget these
     * instances.
     */
    public int Min(){
        // Your code here
        int minDone = Integer.MAX_VALUE;
        for(int i = 0; i < dones.length; i++){
            if(minDone > dones[i]){
                minDone = dones[i];
            }
        }

        mutex.lock();
        for(int i = 0; i < minDone; i++){
            if(allInst.containsKey(i)){
                //Instance currInst = allInst.get(req.instId);
                //currInst.status.state = Forgotten;
                allInst.remove(i);
            }
        }
        mutex.unlock();
        return minDone+1;
    }



    /**
     * the application wants to know whether this
     * peer thinks an instance has been decided,
     * and if so what the agreed value is. Status()
     * should just inspect the local peer state;
     * it should not contact other Paxos peers.
     */
    public retStatus Status(int seq){
        // Your code here
        if(allInst.containsKey(seq)){
            return allInst.get(seq).status;
        } else {
            //System.out.println("Sequence not found: " + seq);
            //return null;
            return new retStatus(State.Pending, null);
        }


    }

    /**
     * helper class for Status() return
     */
    public class retStatus{
        public State state;
        public Object v;

        public retStatus(State state, Object v){
            this.state = state;
            this.v = v;
        }
    }

    /**
     * Tell the peer to shut itself down.
     * For testing.
     * Please don't change these four functions.
     */
    public void Kill(){
        this.dead.getAndSet(true);
        if(this.registry != null){
            try {
                UnicastRemoteObject.unexportObject(this.registry, true);
            } catch(Exception e){
                System.out.println("None reference");
            }
        }
    }

    public boolean isDead(){
        return this.dead.get();
    }

    public void setUnreliable(){
        this.unreliable.getAndSet(true);
    }

    public boolean isunreliable(){
        return this.unreliable.get();
    }


}
