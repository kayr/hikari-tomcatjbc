package org.apache.tomcat.jdbc.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

class ConnectionCleaner implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionCleaner.class.getName());
    static final int CONNECTION_CLEANER_PERIOD_SECS = 30;


    private List<PeConnectionWrapper> connections;
    private String name;

    ConnectionCleaner(List<PeConnectionWrapper> connections, String name) {
        this.connections = connections;
        this.name = name;
    }


    @Override
    public void run() {
        try {
            cleanConnections();
        } catch (Exception x) {
            LOGGER.error("{} - Could not close expired connections: ", x);
        }

    }

    private void cleanConnections() {

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("{} - Connections Before Clean [{}]", name, connections.size());
        }
        for (PeConnectionWrapper con : connections) {
            if (con.isExpired()) {
                try {
                    LOGGER.info("{} - ########################################### ", con.getDbName());
                    LOGGER.info("{} - Connection has passed its life Span.. killing it: {}", con.getDbName(), con);
                    LOGGER.info("{} - ###########################################", con.getDbName());

                    con.close();
                } catch (SQLException e) {
                    LOGGER.error(con.getDbName() + " - Could not close expired connection: ", e);
                }
            }
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(" - Connections After Clean [{}]", connections.size());
        }
    }


}
