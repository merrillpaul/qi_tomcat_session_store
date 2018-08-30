package com.pearson.tomcat.session;

import org.apache.catalina.Context;
import org.apache.catalina.Globals;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Session;
import org.apache.catalina.Store;
import org.apache.catalina.session.StandardSession;
import org.apache.catalina.util.LifecycleBase;
import org.apache.juli.logging.Log;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of the {@link Store Store}
 * interface that stores serialized com.pearson.session objects in mongo.
 * Sessions that are saved are still subject to being expired
 * based on inactivity.
 * <p>
 * Start Local redis with
 * `redis-server `
 * <p>
 * <p>
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

	// ---- Connection Pool attributes ------
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

	///////////////////////////////////////////////////////////////////

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

	/**
	 * Return an integer containing a count of all Sessions
	 * currently saved in this Store.  If there are no Sessions,
	 * <code>0</code> is returned.
	 *
	 * @return the count of all sessions currently saved in this Store
	 * @throws IOException if an input/output error occurred
	 */
	@Override
	public int getSize() throws IOException {
		int size = 0;

		synchronized (this) {
			int numberOfTries = 2;
			while (numberOfTries > 0) {
				Jedis jedis = getResource();

				if (jedis == null) {
					return size;
				}

				try {
					Long count = jedis.zcount(keyName + "_expiry", "-inf", "+inf");
					numberOfTries = 0;
					size = count.intValue();
				} catch (Exception e) {
					getLogger().error(sm.getString(getStoreName() + ".RedisException", e));
				} finally {
					jedis.close();
				}
				numberOfTries--;
			}
		}
		return size;
	}

	@Override
	public String[] expiredKeys() throws IOException {
		return keys(true);
	}

	@Override
	public String[] keys() throws IOException {
		return keys(false);
	}

	/**
	 * Load the Session associated with the id <code>id</code>.
	 * If no such session is found <code>null</code> is returned.
	 *
	 * @param id a value of type <code>String</code>
	 * @return the stored <code>Session</code>
	 * @throws ClassNotFoundException if an error occurs
	 * @throws IOException            if an input/output error occurred
	 */
	@Override
	public Session load(String id) throws ClassNotFoundException, IOException {
		StandardSession _session = null;
		Context context = getManager().getContext();
		Log contextLog = getLogger();

		synchronized (this) {
			int numberOfTries = 2;
			while (numberOfTries > 0) {
				Jedis jedis = getResource();
				if (jedis == null) {
					return null;
				}

				ClassLoader oldThreadContextCL = context.bind(Globals.IS_SECURITY_ENABLED, null);

				try {
					String key = keyName + "_user:" + id;
					List<byte[]> res = jedis.hmget(key.getBytes(), "data".getBytes());

					if (res.size() == 0 || res.get(0) == null) {
						if (contextLog.isDebugEnabled()) {
							contextLog.debug(getStoreName() + ": No persisted data object found");
						}
					} else {
						try (
								InputStream downloadStream = new ByteArrayInputStream(res.get(0));
								ObjectInputStream ois =
										getObjectInputStream(downloadStream)) {

							if (contextLog.isDebugEnabled()) {
								contextLog.debug(sm.getString(
										getStoreName() + ".loading", id, keyName));
							}

							_session = (StandardSession) manager.createEmptySession();
							_session.readObjectData(ois);
							_session.setManager(manager);
						}
					}
					// Break out after the finally block
					numberOfTries = 0;


				} catch (Exception e) {
					contextLog.error(sm.getString(getStoreName() + ".RedisException", e));

				} finally {
					context.unbind(Globals.IS_SECURITY_ENABLED, oldThreadContextCL);
					jedis.close();
				}
				numberOfTries--;
			}
		}

		return _session;
	}

	/**
	 * Remove the Session with the specified session identifier from
	 * this Store, if present.  If no such Session is present, this method
	 * takes no action.
	 *
	 * @param id Session identifier of the Session to be removed
	 *
	 * @exception IOException if an input/output error occurs
	 */
	@Override
	public void remove(String id) throws IOException {
		synchronized (this) {
			int numberOfTries = 2;
			while (numberOfTries > 0) {
				Jedis jedis = getResource();

				if (jedis == null) {
					return;
				}

				try {
					Pipeline pipeline = jedis.pipelined();
					pipeline.del(keyName + "_user:" + id);
					pipeline.zrem(keyName + "_expiry", id);
					pipeline.sync();
					// Break out after the finally block
					numberOfTries = 0;
				} catch (Exception e) {
					getLogger().error(sm.getString(getStoreName() + ".RedisException", e));
				} finally {
					jedis.close();
				}
				numberOfTries--;
			}
		}

		if (getLogger().isDebugEnabled()) {
			getLogger().debug(sm.getString(getStoreName() + ".removing", id, keyName));
		}
	}

	/**
	 * Remove all of the Sessions in this Store.
	 *
	 * @exception IOException if an input/output error occurs
	 */

	@Override
	public void clear() throws IOException {
		synchronized (this) {
			int numberOfTries = 2;
			while (numberOfTries > 0) {
				Jedis jedis = getResource();
				if (jedis == null) {
					return;
				}

				try {
					Set<String> matchingKeys = new HashSet<>();
					ScanParams params = new ScanParams();
					params.match(keyName + "_user:*");

					String nextCursor = "0";

					do {
						ScanResult<String> scanResult = jedis.scan(nextCursor, params);
						List<String> keys = scanResult.getResult();
						nextCursor = scanResult.getStringCursor();

						matchingKeys.addAll(keys);

					} while (!nextCursor.equals("0"));

					Pipeline pipeline = jedis.pipelined();
					pipeline.del(keyName + "_expiry");
					if (matchingKeys.size() > 0) {
						pipeline.del(matchingKeys.toArray(new String[matchingKeys.size()]));
					}
					pipeline.sync();
					// Break out after the finally block
					numberOfTries = 0;
				} catch (Exception e) {
					e.printStackTrace();
					getLogger().error(sm.getString(getStoreName() + ".RedisException", e));
				} finally {
					jedis.close();
				}
				numberOfTries--;
			}
		}
	}

	/**
	 * Saves the pertinent session information as an HMSET and delete and saves a ZADD with the session id
	 * and the expiration time for quick scans later in a pipeline
	 *
	 * @param session
	 * @throws IOException
	 */
	@Override
	public void save(Session session) throws IOException {
		ByteArrayOutputStream bos = null;

		synchronized (this) {
			int numberOfTries = 2;
			while (numberOfTries > 0) {
				Jedis jedis = getResource();
				if (jedis == null) {
					return;
				}

				try {
					String sessionId = session.getIdInternal();
					// remove(session.getIdInternal(), collection);
					Pipeline pipeline = jedis.pipelined();
					// first we remove the id from the zrange
					pipeline.zrem(keyName + "_expiry", sessionId);

					// now we get the session obj as a serialized byte array
					bos = new ByteArrayOutputStream();
					try (ObjectOutputStream oos =
							     new ObjectOutputStream(new BufferedOutputStream(bos))) {
						((StandardSession) session).writeObjectData(oos);
					}
					byte[] obs = bos.toByteArray();

					// `runs the equivalent of hmset qi_sessions_user:<someid> id: the_id data: serialized session obj stream`
					Map<byte[], byte[]> hash = new HashMap<>();

					String key = keyName + "_user:" + sessionId;
					Long expiryTime = session.getLastAccessedTime() + (session.getMaxInactiveInterval() * 1000);
					hash.put("id".getBytes(), sessionId.getBytes());
					hash.put("data".getBytes(), obs);
					hash.put("valid".getBytes(), (session.isValid() ? "1" : "0").getBytes());
					pipeline.hmset(key.getBytes(), hash);

					// we now pipe an additional range id for quick look up later
					pipeline.zadd(keyName + "_expiry", expiryTime.doubleValue(), sessionId);

					// fire it
					pipeline.sync();
					// Break out after the finally block
					numberOfTries = 0;

				} catch (Exception e) {
					e.printStackTrace();
					getLogger().error(sm.getString(getStoreName() + ".RedisStoreException in saving session info", e));
				} finally {
					jedis.close();

				}
				numberOfTries--;
			}
		}

		if (getLogger().isDebugEnabled()) {
			getLogger().debug(sm.getString(getStoreName() + ".saving",
					session.getIdInternal(), keyName));
		}
	}

	/**
	 * Close the specified database connection.
	 */
	protected void close() {
		// Do nothing if the database connection is already closed
		if (this.jedisPool == null) {
			return;
		}
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

	/**
	 * Return an array containing the session identifiers of all Sessions
	 * currently saved in this Store.  If there are no such Sessions, a
	 * zero-length array is returned.
	 *
	 * @param expiredOnly flag, whether only keys of expired sessions should
	 *                    be returned
	 * @return array containing the list of session IDs
	 * @throws IOException if an input/output error occurred
	 */
	private String[] keys(boolean expiredOnly) throws IOException {
		String keys[] = null;
		synchronized (this) {
			int numberOfTries = 2;
			while (numberOfTries > 0) {

				Jedis jedis = getResource();
				if (jedis == null) {
					return new String[0];
				}
				try {
					Set<String> results = null;
					if (!expiredOnly) {
						results = jedis.zrangeByScore(keyName + "_expiry", 0, Double.MAX_VALUE);
					} else {
						results = jedis.zrangeByScore(keyName + "_expiry", 0, System.currentTimeMillis() - 1);
					}


					keys = results.toArray(new String[results.size()]);
					numberOfTries = 0;

				} catch (Exception e) {
					getLogger().error(sm.getString(getStoreName() + ".RedisException", e));
					keys = new String[0];
				} finally {
					jedis.close();
				}
				numberOfTries--;


			}
			return keys;
		}
	}
}
