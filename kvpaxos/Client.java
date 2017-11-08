package kvpaxos;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Random;


public class Client {
    String[] servers;
    int[] ports;

    // Your data here
    Random rand;


    public Client(String[] servers, int[] ports){
        this.servers = servers;
        this.ports = ports;
        // Your initialization code here
        rand = new Random();
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
        KVPaxosRMI stub;
        try{
            Registry registry= LocateRegistry.getRegistry(this.ports[id]);
            stub=(KVPaxosRMI) registry.lookup("KVPaxos");
            if(rmi.equals("Get"))
                callReply = stub.Get(req);
            else if(rmi.equals("Put")){
                callReply = stub.Put(req);}
            else
                System.out.println("Wrong parameters!");
        } catch(Exception e){
            return null;
        }
        return callReply;
    }

    // RMI handlers
    public Integer Get(String key){
        // Your code here
        int clientSeq = rand.nextInt();
        Op op = new Op("Get", clientSeq, key, 0);
        Request req = new Request(op);
        int i = 0;
        while (true) {
           Response rsp = Call("Get", req, i);
           if (rsp != null) {
               return rsp.value;
           }
           i = (i + 1) % (ports.length);
        }
    }

    public boolean Put(String key, Integer value){
        // Your code here
        int clientSeq = rand.nextInt();
        Op op = new Op("Put", clientSeq, key, value);
        Request req = new Request(op);
        int i = 0;
        while (true) {
            Response rsp = Call("Put", req, i);
            if (rsp != null) {
                return rsp.err;
            }
            i = (i + 1) % (ports.length);
        }
    }

}
