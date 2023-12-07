import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * Adjusts the number of nodes based on the current abort rate
 */
public class AdjustWorker implements Runnable {

    Config config;
    List<BlockingQueue<TxStatus>> queues;


    public AdjustWorker(Config config) throws SQLException {
        this.config = config;
        queues = new ArrayList<>();
        for (int i = 0; i < config.adjustWorkers; i++) {
            LinkedBlockingQueue<TxStatus> queue = new LinkedBlockingQueue<>();
            queues.add(queue);
            new Worker(queue);
        }
        (new Thread(this)).start();
    }


    public void run() {
        try {
            Connection connection = DriverManager.getConnection(config.connectionString);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
            double windowBegin = ((double) config.adjustWindow * config.adjustDelta / 100);
            PreparedStatement getStatus = connection.prepareStatement(
                    "SELECT table_name, column_name, pk, commits, aborts, mrv_size(table_name, column_name, pk_sql)" + 
                    "FROM tx_status " + 
                    "WHERE last_updated >= now() - interval '" + windowBegin + " milliseconds'");
            PreparedStatement clear = connection.prepareStatement("DELETE FROM tx_status");
            Random rand = new Random();

            while (true) {
                try {
                    ResultSet rs = getStatus.executeQuery();
                    clear.executeUpdate();
                    connection.commit();
                    while (rs.next()) {
                        TxStatus status = new TxStatus(rs.getString(1), rs.getString(2), rs.getString(3),
                                rs.getInt(4), rs.getInt(5), rs.getInt(6));
                        int randomWorker = rand.nextInt(queues.size());
                        queues.get(randomWorker).add(status);
                    }
                    Thread.sleep(config.adjustDelta);
                }
                catch (Exception e) {
                    connection.rollback();
                    e.printStackTrace();
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }



    class Worker implements Runnable {
        private BlockingQueue<TxStatus> queue;
        private Connection connection;
        private Random rand = new Random();


        public Worker(BlockingQueue<TxStatus> queue) throws SQLException {
            connection = DriverManager.getConnection(config.connectionString);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
            this.queue = queue;
            (new Thread(this)).start();
        }

        
        public void run() {
            while (true) {
                try {
                    TxStatus status = queue.take();
                    int nodes = status.mrvSize;
                    double ar = status.abortRate;
                    long currentMaxNodes = config.maxNodes;
                    System.out.println("AR: " + ar);
                    if (nodes <= currentMaxNodes) {
                        if (ar > config.arGoal && nodes < currentMaxNodes) {
                            addNodes(status, Math.min(Math.round(1 + nodes * ar), currentMaxNodes - nodes));
                        }
                        else if (ar < config.arMin && nodes > config.minNodes) {
                            removeNode(status);
                        }
                    }
                    else {
                        removeNode(status);
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }


        List<List<Integer>> listRksAndValues(TxStatus status) throws SQLException {
            Statement s = connection.createStatement();
            ResultSet rs = s.executeQuery(String.format(
                    "SELECT rk, %s FROM %s_%s WHERE %s",
                    status.columnName, status.tableName, status.columnName, status.pkCond
            ));
            List<List<Integer>> rks = new ArrayList<>();
            while (rs.next()) {
                List<Integer> l = new ArrayList<>(2);
                l.add(rs.getInt(1));
                l.add(rs.getInt(2));
                rks.add(l);
            }
            return rks;
        }


        void addNodes(TxStatus status, long nodes) throws SQLException {
            String insert = String.format(
                    "INSERT INTO %s_%s (%s, rk, %s, valid) VALUES(%s, ?, 0, false)",
                    status.tableName, status.columnName, status.pkColumns, status.columnName, status.pkValues
            );
            PreparedStatement ps = connection.prepareStatement(insert);
            List<Integer> rks = listRksAndValues(status).stream().map(x -> x.get(0)).collect(Collectors.toList());
            List<Integer> availableRks = IntStream.range(0, config.maxNodes).boxed().collect(Collectors.toList());
            availableRks.removeAll(rks);

            for (int i=0; i<nodes; i++) {
                int rk = availableRks.remove(rand.nextInt(availableRks.size()));
                ps.setInt(1, rk);
                ps.addBatch();
            }

            ps.executeBatch();
            connection.commit();
        }

        Integer rkRemove(TxStatus status) throws SQLException {
            Statement s = connection.createStatement();
            ResultSet rs = s.executeQuery(String.format(
                    "SELECT rk FROM %s_%s WHERE %s AND valid = False FOR UPDATE",
                    status.tableName, status.columnName, status.pkCond
            ));

            List<Integer> rks = new ArrayList<>();
            while (rs.next()) {
                rks.add(rs.getInt(1));
            }

            Integer toRemove = null;
            if (rks.size() > 0)
                toRemove = rks.remove(rand.nextInt(rks.size()));

            return toRemove;
        }


        void removeNode(TxStatus status) throws SQLException {
            
            String remove = String.format(
                    "DELETE FROM %s_%s WHERE %s AND rk = ? AND valid = False",
                    status.tableName, status.columnName, status.pkCond
            );
            PreparedStatement removePs = connection.prepareStatement(remove);

            boolean done = false;
            while (!done) {
                try {
                    Integer toRemove = rkRemove(status);
                    if (toRemove != null) {
                        removePs.setInt(1, toRemove);
                        removePs.execute();
                        connection.commit();
                    }
                    done = true;
                } catch (SQLException e) {
                    connection.rollback();
                    e.printStackTrace();
                } 
            }
        }
    }

}
