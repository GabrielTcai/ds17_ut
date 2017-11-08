package kvpaxos;
import paxos.Paxos;
import paxos.State;
// You are allowed to call Paxos.Status to check if agreement was made.

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class Server implements KVPaxosRMI {

    ReentrantLock mutex;
    Registry registry;
    Paxos px;
    int me;

    String[] servers;
    int[] ports;
    KVPaxosRMI stub;

    // Your definitions here
    //ArrayList<Op> logList;
    Map<String, Integer> kvStore;
    Map<Integer, Boolean> servedSeq;
    int lastSeq;

    public Server(String[] servers, int[] ports, int me){
        this.me = me;
        this.servers = servers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.px = new Paxos(me, servers, ports);
        // Your initialization code here
        kvStore = new HashMap<String, Integer>();
        servedSeq = new HashMap<Integer, Boolean>();
        //this.logList = new ArrayList<Op>();
        this.lastSeq = 0;

        try{
            System.setProperty("java.rmi.server.hostname", this.servers[this.me]);
            registry = LocateRegistry.getRegistry(this.ports[this.me]);
            stub = (KVPaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("KVPaxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    public boolean addLog(Op op){
        //logList.add(op);
        Boolean err = false;
        if(op.op.equals("Put")){
            if (!kvStore.containsKey(op.key)) {
                kvStore.put(op.key, op.value);
                err = true;
            } else {
                err = false;
            }
        }
        px.Done(lastSeq++);
        //if (lastSeq % 1024 == 0) {
        //    servedSeq.clear();
        //}
        servedSeq.put(op.ClientSeq, err);
        return err;
    }

    public Boolean sync(Op op){
        boolean done = false;
        Boolean err = false;
        Op currOp;
        while(!done){
            if (servedSeq.containsKey(op.ClientSeq)) {
                err = servedSeq.get(op.ClientSeq);
                break;
            }
            Paxos.retStatus ret;
            ret = px.Status(lastSeq);
            if(ret.state == State.Decided){
                currOp = Op.class.cast(ret.v);
            } else {
                px.Start(lastSeq, op);
                currOp = wait(lastSeq);
            }
            err = addLog(currOp);
            done = (currOp.ClientSeq == op.ClientSeq);
        }
        return err;
    }

    public Op wait(int seq){
        int to = 10;
        while(true){
            Paxos.retStatus ret = this.px.Status(seq);
            if(ret.state == State.Decided){
                return Op.class.cast(ret.v);
            }
            try{
                Thread.sleep(to);
            } catch (Exception e){
                e.printStackTrace();
            }
            if(to < 1000){
                to = to * 2;
            }
        }
    }
    // RMI handlers
    public Response Get(Request req){
        // Your code here
        mutex.lock();
        sync(req.op);
        Integer value;
        boolean err;
        if (kvStore.containsKey(req.op.key)) {
            value = kvStore.get(req.op.key);
            err = true;
        } else{
            value = -1;
            err = false;
        }
        mutex.unlock();
        return new Response(value, err);
    }

    public Response Put(Request req){
        // Your code here
        mutex.lock();
        boolean err;
        err = sync(req.op);
        mutex.unlock();
        return new Response(0, err);
    }


}
