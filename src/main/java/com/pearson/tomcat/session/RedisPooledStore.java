package com.pearson.tomcat.session;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.Session;
import org.apache.catalina.Store;
import org.apache.catalina.util.LifecycleBase;
import org.apache.juli.logging.Log;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.time.Duration;

/**
 * Implementation of the {@link Store Store}
 * interface that stores serialized com.pearson.session objects in mongo.
 * Sessions that are saved are still subject to being expired
 * based on inactivity.
 *
 * Start Local redis with
 * `redis-server `
 *
 *
 * <p>
 * default config in com.pearson.session.tomcat manager
 * <code>
 * <Store className="com.pearson.tomcat.session.RedisPooledStore"
 * redisHost="localhost"
 * redisPort="6379"
 * />
 * <p>
 * </code>
 * <p>
 * or full blown using
 * * <code>
 * <Store className="com.pearson.tomcat.session.RedisPooledStore"
 * redisHost="host"
 * redisPort="8888"
 * keyName="sessionKey"
 * maxTotal="128"
 * maxIdle="128"
 * minIdle="app"
 * testOnBorrow="true"
 * testOnReturn="true"
 * testWhileIdle="true"
 * minEvictableIdleTime="100" // seconds
 * timeBetweenEvictionRuns="50"// seconds
 * numTestsPerEvictionRun="3"
 * blockWhenExhausted="true"
 * />
 * <p>
 * </code>
 * This uses a qi_sessions collection inside
 */
public class RedisPooledStore extends AbstractStore {

	private Log getLogger() {
		return getManager().getContext().getLogger();
	}

	protected String keyName = "qi_sessions";

	protected String redisHost = "localhost";

	protected int redisPort = 6379;

	protected int maxTotal = 128;

	protected int maxIdle = 128;

	protected int minIdle = 5;

	protected boolean testOnBorrow = true;

	protected boolean testOnReturn = true;

	protected boolean testWhileIdle = true;

	protected int minEvictableIdleTime = 60; // seconds

	protected int timeBetweenEvictionRuns = 30; // seconds

	protected int numTestsPerEvictionRun = 3;

	protected boolean blockWhenExhausted = true;

	private JedisPool jedisPool;

	protected Jedis getResource() {

		if (jedisPool == null) {
			buildJedisPool();
		}
		return jedisPool.getResource();
	}


	// -------------------------------------------------------------- Properties

	public String getKeyName() {
		return keyName;
	}

	public void setKeyName(String keyName) {
		String oldKeyName = this.keyName;
		this.keyName = keyName;
		support.firePropertyChange("keyName", oldKeyName, this.keyName);
	}

	public String getRedisHost() {
		return redisHost;
	}

	public void setRedisHost(String redisHost) {
		String oldredisHost = this.redisHost;
		this.redisHost = redisHost;
		support.firePropertyChange("redisHost", oldredisHost, this.redisHost);
	}

	public int getRedisPort() {
		return redisPort;
	}

	public void setRedisPort(int redisPort) {
		int oldredisPort = this.redisPort;
		this.redisPort = redisPort;
		support.firePropertyChange("redisPort", oldredisPort, this.redisPort);
	}

	public int getMaxTotal() {
		return maxTotal;
	}

	public void setMaxTotal(int maxTotal) {
		int oldmaxTotal = this.maxTotal;
		this.maxTotal = maxTotal;
		support.firePropertyChange("maxTotal", oldmaxTotal, this.maxTotal);
	}

	public int getMaxIdle() {
		return maxIdle;
	}

	public void setMaxIdle(int maxIdle) {
		int oldmaxIdle = this.maxIdle;
		this.maxIdle = maxIdle;
		support.firePropertyChange("maxIdle", oldmaxIdle, this.maxIdle);
	}

	public int getMinIdle() {
		return minIdle;
	}

	public void setMinIdle(int minIdle) {
		int oldminIdle = this.minIdle;
		this.minIdle = minIdle;
		support.firePropertyChange("minIdle", oldminIdle, this.minIdle);
	}

	public boolean isTestOnBorrow() {
		return testOnBorrow;
	}

	public void setTestOnBorrow(boolean testOnBorrow) {
		boolean oldtestOnBorrow = this.testOnBorrow;
		this.testOnBorrow = testOnBorrow;
		support.firePropertyChange("testOnBorrow", oldtestOnBorrow, this.testOnBorrow);
	}

	public boolean isTestOnReturn() {
		return testOnReturn;
	}

	public void setTestOnReturn(boolean testOnReturn) {
		boolean oldtestOnReturn = this.testOnReturn;
		this.testOnReturn = testOnReturn;
		support.firePropertyChange("testOnReturn", oldtestOnReturn, this.testOnReturn);
	}

	public boolean isTestWhileIdle() {
		return testWhileIdle;
	}

	public void setTestWhileIdle(boolean testWhileIdle) {
		boolean oldtestWhileIdle = this.testWhileIdle;
		this.testWhileIdle = testWhileIdle;
		support.firePropertyChange("testWhileIdle", oldtestWhileIdle, this.testWhileIdle);
	}

	public int getMinEvictableIdleTime() {
		return minEvictableIdleTime;
	}

	public void setMinEvictableIdleTime(int minEvictableIdleTime) {
		int oldminEvictableIdleTime = this.minEvictableIdleTime;
		this.minEvictableIdleTime = minEvictableIdleTime;
		support.firePropertyChange("minEvictableIdleTime", oldminEvictableIdleTime, this.minEvictableIdleTime);
	}

	public int getTimeBetweenEvictionRuns() {
		return timeBetweenEvictionRuns;
	}

	public void setTimeBetweenEvictionRuns(int timeBetweenEvictionRuns) {
		int oldtimeBetweenEvictionRuns = this.timeBetweenEvictionRuns;
		this.timeBetweenEvictionRuns = timeBetweenEvictionRuns;
		support.firePropertyChange("timeBetweenEvictionRuns", oldtimeBetweenEvictionRuns, this.timeBetweenEvictionRuns);
	}

	public int getNumTestsPerEvictionRun() {
		return numTestsPerEvictionRun;
	}

	public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
		int oldnumTestsPerEvictionRun = this.numTestsPerEvictionRun;
		this.numTestsPerEvictionRun = numTestsPerEvictionRun;
		support.firePropertyChange("numTestsPerEvictionRun", oldnumTestsPerEvictionRun, this.numTestsPerEvictionRun);
	}

	public boolean isBlockWhenExhausted() {
		return blockWhenExhausted;
	}

	public void setBlockWhenExhausted(boolean blockWhenExhausted) {
		boolean oldblockWhenExhausted = this.blockWhenExhausted;
		this.blockWhenExhausted = blockWhenExhausted;
		support.firePropertyChange("blockWhenExhausted", oldblockWhenExhausted, this.blockWhenExhausted);
	}

	// -------------------------------------------------------------------------

	/**
	 * @return the thread name for this Store.
	 */
	public String getThreadName() {
		return "RedisPooledStore";
	}

	/**
	 * @return the name for this Store, used for logging.
	 */
	@Override
	public String getStoreName() {
		return "RedisPooledStore";
	}

	@Override
	public int getSize() throws IOException {
		return 0;
	}

	@Override
	public String[] keys() throws IOException {
		return new String[0];
	}

	@Override
	public Session load(String id) throws ClassNotFoundException, IOException {
		return null;
	}

	@Override
	public void remove(String id) throws IOException {

	}

	@Override
	public void clear() throws IOException {

	}

	/**
	 * Saves the pertinent session information as an HMSET and delete and saves a ZADD with the session id
	 * and the expiration time for quick scans later in a pipeline
	 * @param session
	 * @throws IOException
	 */
	@Override
	public void save(Session session) throws IOException {

	}

	/**
	 * Close the specified database connection.
	 *
	 *
	 */
	protected void close() {

		// Do nothing if the database connection is already closed
		if (this.jedisPool == null)
			return;
		this.jedisPool.close();
		this.jedisPool = null;

	}

	/**
	 * Start this component and implement the requirements
	 * of {@link LifecycleBase#startInternal()}.
	 *
	 * @throws LifecycleException if this component detects a fatal error
	 *                            that prevents this component from being used
	 */
	@Override
	protected synchronized void startInternal() throws LifecycleException {

		this.buildJedisPool();
		super.startInternal();
	}

	/**
	 * Stop this component and implement the requirements
	 * of {@link LifecycleBase#stopInternal()}.
	 *
	 * @throws LifecycleException if this component detects a fatal error
	 *                            that prevents this component from being used
	 */
	@Override
	protected synchronized void stopInternal() throws LifecycleException {

		super.stopInternal();
		this.close();

	}


	//+---------- PRIVATE

	private void buildJedisPool() {
		final JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(this.maxTotal);
		poolConfig.setMaxIdle(this.maxIdle);
		poolConfig.setMinIdle(this.minIdle);
		poolConfig.setTestOnBorrow(this.testOnBorrow);
		poolConfig.setTestOnReturn(this.testOnReturn);
		poolConfig.setTestWhileIdle(this.testWhileIdle);
		poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(this.minEvictableIdleTime).toMillis());
		poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(this.timeBetweenEvictionRuns).toMillis());
		poolConfig.setNumTestsPerEvictionRun(this.numTestsPerEvictionRun);
		poolConfig.setBlockWhenExhausted(this.blockWhenExhausted);
		this.jedisPool = new JedisPool(poolConfig, this.redisHost, this.redisPort);
	}
}
