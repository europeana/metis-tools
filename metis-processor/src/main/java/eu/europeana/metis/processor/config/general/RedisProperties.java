package eu.europeana.metis.processor.config.general;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class RedisProperties {

    //Redis
    @Value("${redis.host}")
    private String redisHost;
    @Value("${redis.port}")
    private int redisPort;
    @Value("${redis.username}")
    private String redisUsername;
    @Value("${redis.password}")
    private String redisPassword;
    @Value("${redis.enableSSL}")
    private boolean redisEnableSSL;
    @Value("${redis.enable.custom.truststore}")
    private boolean redisEnableCustomTruststore;
    @Value("${redisson.connection.pool.size}")
    private int redissonConnectionPoolSize;
    @Value("${redisson.lock.watchdog.timeout.in.secs}")
    private int redissonLockWatchdogTimeoutInSecs;
    @Value("${redisson.connect.timeout.in.millisecs}")
    private int redissonConnectTimeoutInMillisecs;
    @Value("${redisson.dns.monitor.interval.in.millisecs}")
    private int redissonDnsMonitorIntervalInMillisecs;
    @Value("${redisson.idle.connection.timeout.in.millisecs}")
    private int redissonIdleConnectionTimeoutInMillisecs;
    @Value("${redisson.retry.attempts}")
    private int redissonRetryAttempts;

    public String getRedisHost() {
        return redisHost;
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    public String getRedisUsername() {
        return redisUsername;
    }

    public void setRedisUsername(String redisUsername) {
        this.redisUsername = redisUsername;
    }

    public String getRedisPassword() {
        return redisPassword;
    }

    public void setRedisPassword(String redisPassword) {
        this.redisPassword = redisPassword;
    }

    public boolean isRedisEnableSSL() {
        return redisEnableSSL;
    }

    public void setRedisEnableSSL(boolean redisEnableSSL) {
        this.redisEnableSSL = redisEnableSSL;
    }

    public boolean isRedisEnableCustomTruststore() {
        return redisEnableCustomTruststore;
    }

    public void setRedisEnableCustomTruststore(boolean redisEnableCustomTruststore) {
        this.redisEnableCustomTruststore = redisEnableCustomTruststore;
    }

    public int getRedissonConnectionPoolSize() {
        return redissonConnectionPoolSize;
    }

    public void setRedissonConnectionPoolSize(int redissonConnectionPoolSize) {
        this.redissonConnectionPoolSize = redissonConnectionPoolSize;
    }

    public int getRedissonLockWatchdogTimeoutInSecs() {
        return redissonLockWatchdogTimeoutInSecs;
    }

    public void setRedissonLockWatchdogTimeoutInSecs(int redissonLockWatchdogTimeoutInSecs) {
        this.redissonLockWatchdogTimeoutInSecs = redissonLockWatchdogTimeoutInSecs;
    }

    public int getRedissonConnectTimeoutInMillisecs() {
        return redissonConnectTimeoutInMillisecs;
    }

    public void setRedissonConnectTimeoutInMillisecs(int redissonConnectTimeoutInMillisecs) {
        this.redissonConnectTimeoutInMillisecs = redissonConnectTimeoutInMillisecs;
    }

    public int getRedissonDnsMonitorIntervalInMillisecs() {
        return redissonDnsMonitorIntervalInMillisecs;
    }

    public void setRedissonDnsMonitorIntervalInMillisecs(int redissonDnsMonitorIntervalInMillisecs) {
        this.redissonDnsMonitorIntervalInMillisecs = redissonDnsMonitorIntervalInMillisecs;
    }

    public int getRedissonIdleConnectionTimeoutInMillisecs() {
        return redissonIdleConnectionTimeoutInMillisecs;
    }

    public void setRedissonIdleConnectionTimeoutInMillisecs(int redissonIdleConnectionTimeoutInMillisecs) {
        this.redissonIdleConnectionTimeoutInMillisecs = redissonIdleConnectionTimeoutInMillisecs;
    }

    public int getRedissonRetryAttempts() {
        return redissonRetryAttempts;
    }

    public void setRedissonRetryAttempts(int redissonRetryAttempts) {
        this.redissonRetryAttempts = redissonRetryAttempts;
    }
}
