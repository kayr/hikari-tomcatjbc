getRemoveAbandonedTimeout =  number of seconds a connection is left before it is autoCloses
getValidationQuery =

pool_max_active  :  maxPoolSize
pool_initial_size : minPoolSize


pool_remove_abandoned_timeout : peAutoCloseTime
connection_lifetime_property : setInCode



Most of the tomcat configs are ignored and below the the ones used
------------------------------

config.setDriverClassName(poolProperties.getDriverClassName());
config.setPoolName(poolProperties.getPoolName());
config.setJdbcUrl(poolProperties.getUrl());
config.setUsername(poolProperties.getUsername());
config.setPassword(poolProperties.getPassword());

POOL SIZE
------------------------------
config.setMinimumIdle(Math.min(tomcatConfig.getMinIdle(), tomcatConfig.getInitialSize()));
config.setMaximumPoolSize(poolProperties.getMaxActive());

if (poolProperties.getValidationQuery() == null) {
    config.setConnectionTestQuery("SELECT 1");
} else {
    config.setConnectionTestQuery(poolProperties.getValidationQuery());
}


TIME OUTS
--------------------------------
config.setLeakDetectionThreshold(TimeUnit.SECONDS.toMillis(tomcatConfig.getSuspectTimeout()));

config.setMaxLifetime(tomcatConfig.getMaxAge());

-- Connection Cleaner
   poolConfiguration.getRemoveAbandonedTimeout(); // this is enabled by defualt
        if abandoned timeout <= 0 then we set the default 30 secs
        otherwise we use the default set value



