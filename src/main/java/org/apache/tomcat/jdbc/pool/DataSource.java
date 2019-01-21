package org.apache.tomcat.jdbc.pool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.UtilityElf;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
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

    private static final ScheduledExecutorService scheduledExecutor = new ScheduledThreadPoolExecutor(10, new UtilityElf.DefaultThreadFactory("awamo-connection-housekeeper", true), new ThreadPoolExecutor.DiscardPolicy());
    private final List<PeConnectionWrapper> connections = new CopyOnWriteArrayList<>();


    private HikariDataSource hikariDataSource;
    private PoolConfiguration poolProperties;


    /**
     * //Ignored But Set in Mifos
     * ================================================
     * poolConfiguration.setValidationInterval(tenantConnectionObj.getValidationInterval());
     * <p>
     * poolConfiguration.setRemoveAbandoned(tenantConnectionObj.isRemoveAbandoned());
     * poolConfiguration.setRemoveAbandonedTimeout(tenantConnectionObj.getRemoveAbandonedTimeout());
     * poolConfiguration.setLogAbandoned(tenantConnectionObj.isLogAbandoned());
     * poolConfiguration.setAbandonWhenPercentageFull(tenantConnectionObj.getAbandonWhenPercentageFull());
     * <p>
     * //this are ignored by mifos so we will use the defaults in hikari
     * //=====================================================================
     * // poolConfiguration.setMaxActive(tenant.getMaxActive()); = makes No Sense in Hikari
     * // poolConfiguration.setMinIdle(tenant.getMinIdle()); = minimumIdle
     * // poolConfiguration.setMaxIdle(tenant.getMaxIdle()); = maximumPoolSize
     * <p>
     * // poolConfiguration.setSuspectTimeout(tenant.getSuspectTimeout()); //simialar to logging stale connection
     * // poolConfiguration.setTimeBetweenEvictionRunsMillis(tenant.getTimeBetweenEvictionRunsMillis()); //hikari always runs every 30secs
     * // poolConfiguration.setMinEvictableIdleTimeMillis(tenant.getMinEvictableIdleTimeMillis()); //hikari auto clean after idle timeout
     */

    public DataSource(PoolConfiguration poolProperties) {

        LOG.warn(MSG_LINE);
        LOG.warn("STARTING POOL: {}", poolProperties.getPoolName());
        LOG.warn(MSG_LINE);
        this.poolProperties = poolProperties;
        Properties properties = DatabaseSettings.readDbProperties();

        HikariConfig config = new HikariConfig(properties);

        config.setDriverClassName(poolProperties.getDriverClassName());
        config.setPoolName(poolProperties.getPoolName());
        config.setJdbcUrl(poolProperties.getUrl());
        config.setUsername(poolProperties.getUsername());
        config.setPassword(poolProperties.getPassword());

        config.setMaximumPoolSize(poolProperties.getMaxActive());
        config.setMinimumIdle(poolProperties.getInitialSize());
        config.setScheduledExecutor(scheduledExecutor);


        if (poolProperties.getValidationQuery() == null) {
            config.setConnectionTestQuery("SELECT 1");
        } else {
            config.setConnectionTestQuery(poolProperties.getValidationQuery());
        }

        startConnectionCleaner(connections, poolProperties.getPoolName());

        hikariDataSource = new HikariDataSource(config);

        LOG.warn(MSG_LINE);
        LOG.warn("FINISHED POOL CREATION: {}", poolProperties.getPoolName());
        LOG.warn(MSG_LINE);

    }


    private static void startConnectionCleaner(List<PeConnectionWrapper> connections, String poolName) {
        LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        LOG.info("Starting connection clean for [{}]",poolName);
        scheduledExecutor.scheduleAtFixedRate(new ConnectionCleaner(connections, poolName), 0, ConnectionCleaner.CONNECTION_CLEANER_PERIOD_SECS, TimeUnit.SECONDS);
    }

    public Connection getConnection() throws SQLException {

        return wrapConnection(hikariDataSource.getConnection());
    }

    public Connection getConnection(String username, String password) throws SQLException {
        Connection connection = hikariDataSource.getConnection(username, password);

        return wrapConnection(connection);
    }

    private PeConnectionWrapper wrapConnection(Connection connection) {
        return PeConnectionWrapper.from(hikariDataSource.getPoolName(),
                connection,
                poolProperties.getRemoveAbandonedTimeout(),
                TimeUnit.SECONDS,
                connections);
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
