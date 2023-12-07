import java.sql.*;


/**
 * Refreshes the counter values
 */
public class RefreshWorker implements Runnable {

    Config config;
    Connection connection;

    public RefreshWorker(Config config) throws SQLException {
        this.config = config;
        connection = DriverManager.getConnection(config.connectionString);
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        (new Thread(this)).start();
    }

    public void run() {
        try {
            Connection connection = DriverManager.getConnection(config.connectionString);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
            
            PreparedStatement getPk;
            ResultSet pkSet;
            ResultSetMetaData pkSetMetaData;
            PreparedStatement refresh;
            StringBuilder sb;
            int pkSetLength;

            while (true) {
                for (String table_name: config.refreshTables){
                    try {
                        getPk = connection.prepareStatement("SELECT * FROM " + table_name + "_pk");
                        pkSet = getPk.executeQuery();
                        pkSetMetaData = pkSet.getMetaData();
                        pkSetLength = pkSetMetaData.getColumnCount();

                        while (pkSet.next()) {
                            sb = new StringBuilder();
                            for (int i = 1; i <= pkSetLength; i++) {
                                if (i > 1) sb.append(",  ");
                                sb.append(pkSet.getString(i));
                            }
                            refresh = connection.prepareCall("SELECT refresh_" + table_name + "(" + sb.toString() + ")");
                            refresh.execute();
                            connection.commit();
                        }
                        
                    }
                    catch (Exception e) {
                        connection.rollback();
                        e.printStackTrace();
                    }
                }
                Thread.sleep(config.refreshDelta);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
