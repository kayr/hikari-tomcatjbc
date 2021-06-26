package org.apache.tomcat.jdbc.pool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.UtilityElf;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.security.Policy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class DataSource implements javax.sql.DataSource {

    private static final String MSG_LINE = "==========================================";
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(DataSource.class);

    private static final ScheduledExecutorService scheduledExecutor = new ScheduledThreadPoolExecutor(10,
            new UtilityElf.DefaultThreadFactory("awamo-connection-housekeeper", true),
            new ThreadPoolExecutor.DiscardPolicy());
    private final List<PeConnectionWrapper> connections = new CopyOnWriteArrayList<>();

    private HikariDataSource hikariDataSource;
    private PoolConfiguration poolProperties;

    public DataSource(PoolConfiguration tomcatConfig) {

        LOG.warn(MSG_LINE);
        LOG.warn("STARTING POOL: {}", tomcatConfig.getPoolName());
        LOG.warn(MSG_LINE);
        this.poolProperties = tomcatConfig;

        HikariConfig config = new HikariConfig();

        config.setDriverClassName(tomcatConfig.getDriverClassName());
        config.setPoolName(tomcatConfig.getPoolName());
        config.setJdbcUrl(tomcatConfig.getUrl());
        config.setUsername(tomcatConfig.getUsername());
        config.setPassword(tomcatConfig.getPassword());
        
        config.setDataSourceProperties(tomcatConfig.getDbProperties());
        config.setScheduledExecutor(scheduledExecutor);

        // Set POOL Size
        config.setMinimumIdle(Math.min(tomcatConfig.getMinIdle(), tomcatConfig.getInitialSize()));
        config.setMaximumPoolSize(tomcatConfig.getMaxIdle() + tomcatConfig.getMaxActive());

        


        //Connection life time
        config.setMaxLifetime(tomcatConfig.getMaxAge());
        config.setLeakDetectionThreshold(TimeUnit.SECONDS.toMillis(tomcatConfig.getSuspectTimeout()));

        if (tomcatConfig.getValidationQuery() == null) {
            config.setConnectionTestQuery("SELECT 1");
        } else {
            config.setConnectionTestQuery(tomcatConfig.getValidationQuery());
        }

        startConnectionCleaner(connections, tomcatConfig);

        hikariDataSource = new HikariDataSource(config);

        LOG.warn(MSG_LINE);
        LOG.warn("FINISHED POOL CREATION: {}", tomcatConfig.getPoolName());
        LOG.warn(MSG_LINE);

    }

    private static void startConnectionCleaner(List<PeConnectionWrapper> connections,
            PoolConfiguration poolConfiguration) {
        LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        LOG.info("Starting connection clean for [{}]", poolConfiguration.getPoolName());
        int cofigTimeOutSecs = poolConfiguration.getRemoveAbandonedTimeout();


        int finalSeconds = cofigTimeOutSecs;

        if (cofigTimeOutSecs <= 0) {
            finalSeconds = ConnectionCleaner.CONNECTION_CLEANER_PERIOD_SECS;
        }

        scheduledExecutor.scheduleAtFixedRate(new ConnectionCleaner(connections, poolConfiguration.getPoolName()), 0,
                finalSeconds, TimeUnit.SECONDS);
    }

    public Connection getConnection() throws SQLException {

        return wrapConnection(hikariDataSource.getConnection());
    }

    public Connection getConnection(String username, String password) throws SQLException {
        Connection connection = hikariDataSource.getConnection(username, password);

        return wrapConnection(connection);
    }

    private PeConnectionWrapper wrapConnection(Connection connection) {
        return PeConnectionWrapper.from(hikariDataSource.getPoolName(), connection,
                poolProperties.getRemoveAbandonedTimeout(), TimeUnit.SECONDS, connections);
    }

    public PrintWriter getLogWriter() throws SQLException {
        return hikariDataSource.getLogWriter();
    }

    public void setLogWriter(PrintWriter out) throws SQLException {
        hikariDataSource.setLogWriter(out);
    }

    public void setLoginTimeout(int seconds) throws SQLException {
        hikariDataSource.setLoginTimeout(seconds);
    }

    public int getLoginTimeout() throws SQLException {
        return hikariDataSource.getLoginTimeout();
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return hikariDataSource.getParentLogger();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return hikariDataSource.unwrap(iface);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return hikariDataSource.isWrapperFor(iface);
    }

}
