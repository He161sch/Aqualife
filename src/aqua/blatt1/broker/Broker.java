package aqua.blatt1.broker;

import aqua.blatt1.common.Direction;
import aqua.blatt1.common.FishModel;
import aqua.blatt1.common.msgtypes.*;
import aqua.blatt2.broker.PoisonPill;
import aqua.blatt2.broker.Poisoner;
import messaging.Endpoint;
import messaging.Message;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.sql.SQLOutput;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Broker {

    private Endpoint endpoint;
    private ClientCollection<InetSocketAddress> clients;
    private int id = 0;
    private int POOL_SIZE = 3;
    private ExecutorService executor = Executors.newFixedThreadPool(POOL_SIZE);
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean stopRequest = false;

    public Broker() {
        endpoint = new Endpoint(4711);
        clients = new ClientCollection<InetSocketAddress>();

    }

    private class BrokerTask implements Runnable {
        Message message;

        private BrokerTask(Message message) {
            this.message = message;
        }

        @Override
        public void run() {
            Serializable s = message.getPayload();

            if (s instanceof RegisterRequest) {
                register(message);
            } else if (s instanceof DeregisterRequest) {
                deregister(message);
            } else if (s instanceof HandoffRequest) {
                handoffFish(message);
            }
        }
    }

    public void broker() {
        while (!stopRequest) {
            Message m = endpoint.blockingReceive();
            if(m.getPayload() instanceof PoisonPill)
                break;
            executor.execute(new BrokerTask(m));
        }
        executor.shutdown();
    }

    public void register(Message msg) {
        String idName = "Tank" + id;
        lock.writeLock().lock();
        boolean first = clients.size() == 0;
        clients.add(idName, msg.getSender());
        lock.writeLock().unlock();

        lock.readLock().lock();
        updateNeighbor(msg.getSender());
        updateNeighbor(clients.getLeftNeighorOf(clients.indexOf(msg.getSender())));
        updateNeighbor(clients.getRightNeighorOf(clients.indexOf(msg.getSender())));
        lock.readLock().unlock();

        if(first) {
            endpoint.send(msg.getSender(), new Token());
        }



        endpoint.send(msg.getSender(), new RegisterResponse(idName));

        id++;
        System.out.println("Register: " + msg.getSender().toString());

    }

    public void deregister(Message msg) {
        lock.writeLock().lock();
        InetSocketAddress left = clients.getLeftNeighorOf(clients.indexOf(msg.getSender()));
        InetSocketAddress right = clients.getRightNeighorOf(clients.indexOf(msg.getSender()));

        clients.remove(clients.indexOf(msg.getSender()));

        if (clients.size() > 0) {
            updateNeighbor(left);
            updateNeighbor(right);
        }
        lock.writeLock().unlock();

        System.out.println("Deregister: " + msg.getSender().toString());
    }

    public void handoffFish(Message msg) {
        HandoffRequest s = (HandoffRequest) msg.getPayload();
        FishModel f = s.getFish();
        Direction direction = f.getDirection();
        InetSocketAddress reciever = null;
        lock.readLock().lock();
        if (direction == Direction.LEFT) {
            reciever = clients.getLeftNeighorOf(clients.indexOf(msg.getSender()));
        } else if (direction == Direction.RIGHT) {
            reciever = clients.getRightNeighorOf(clients.indexOf(msg.getSender()));
        }
        lock.readLock().unlock();
        endpoint.send(reciever, msg.getPayload());
        System.out.println("handoff");
    }

    private void updateNeighbor(InetSocketAddress client) {
        int idx = clients.indexOf(client);
        endpoint.send(clients.getClient(idx), new NeighborUpdate(clients.getLeftNeighorOf(idx), clients.getRightNeighorOf(idx)));
    }

    public static void main(String[] args) {
        Broker b = new Broker();

        b.broker();
    }
}
