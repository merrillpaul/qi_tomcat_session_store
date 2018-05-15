package com.pearson.tomcat.session;

import com.mongodb.Block;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCompressor;
import com.mongodb.MongoCredential;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.model.Projections;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import org.apache.catalina.Container;
import org.apache.catalina.Context;
import org.apache.catalina.Globals;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Session;
import org.apache.catalina.Store;
import org.apache.catalina.session.StandardSession;
import org.apache.catalina.session.StoreBase;
import org.apache.catalina.util.LifecycleBase;
import org.apache.juli.logging.Log;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.where;

/**
 * Implementation of the {@link Store Store}
 * interface that stores serialized com.pearson.session objects in mongo.
 * Sessions that are saved are still subject to being expired
 * based on inactivity.
 *
 * Start Mongo with
 * `mongod --dbpath ~/mongo/data/db --enableMajorityReadConcern `
 *
 *
 * <p>
 * default config in com.pearson.session.tomcat manager
 * <code>
 * <Store className="com.pearson.tomcat.session.MongoStore"
 * connectionURL="mongodb://hostOne:27017,hostTwo:27017"
 * dbName="somedb"
 * username="tomcat_sessions_user"
 * password=""Password1!
 * />
 * <p>
 * </code>
 * <p>
 * or full blown using
 * * <code>
 * <Store className="com.pearson.tomcat.session.MongoStore"
 * connectionURL="mongodb://hostOne:27017,hostTwo:27017"
 * dbName="somedb"
 * username="tomcat_sessions_user"
 * password=""Password1!
 * sessionCollection="someCollectionName"
 * sessionAppCol="app"
 * <p>
 * />
 * <p>
 * </code>
 * This uses a qi_sessions collection inside
 */
public class MongoStore extends StoreBase {

	private Log getLogger() {
		return getManager().getContext().getLogger();
	}

	/**
	 * Context name associated with this Store
	 */
	private String name = null;
	/**
	 * Name to register for this Store, used for logging.
	 */
	protected static final String storeName = "MongoStore";
	/**
	 * Name to register for the background thread.
	 */
	protected static final String threadName = "MongoStore";
	/**
	 * Connection to mongo
	 */
	protected MongoCollection collection = null;
	/**
	 * The connection username to use when trying to connect to the database.
	 */
	protected String username = null;
	/**
	 * The connection URL to use when trying to connect to the database.
	 */
	protected String password = null;
	/**
	 * The mongo db name
	 */
	protected String dbName = null;
	/**
	 * connectionURL
	 */
	protected String connectionURL = null;
	/**
	 * Collection to use.
	 */
	protected String sessionCollection = "qi_sessions";
	/**
	 * Column to use for /Engine/Host/Context name
	 */
	protected String sessionAppCol = "app";
	/**
	 * Data column to use.
	 */
	protected String sessionDataCol = "data";
	/**
	 * {@code Is Valid} column to use.
	 */
	protected String sessionValidCol = "valid";
	/**
	 * Max Inactive column to use.
	 */
	protected String sessionMaxInactiveCol = "maxinactive";
	/**
	 * Last Accessed column to use.
	 */
	protected String sessionLastAccessedCol = "lastaccess";

	/**
	 * Id column to use.
	 */
	protected String sessionIdCol = "id";


	protected int poolMinSize = 5;


	protected int poolMaxSize = 100;

	protected int poolMaxWaitTime = 150; //ms

	protected int poolMaxConnectionLifeTime = 300; //ms

	protected int poolMaxConnectionIdleTime = 200; //ms


	private MongoDatabase mongoDatabase;

	private GridFSBucket gridFSFilesBucket;
	/**
	 * @return the name for this instance (built from container name)
	 */
	public String getName() {
		if (name == null) {
			Container container = manager.getContext();
			String contextName = container.getName();
			if (!contextName.startsWith("/")) {
				contextName = "/" + contextName;
			}
			String hostName = "";
			String engineName = "";

			if (container.getParent() != null) {
				Container host = container.getParent();
				hostName = host.getName();
				if (host.getParent() != null) {
					engineName = host.getParent().getName();
				}
			}
			name = "/" + engineName + "/" + hostName + contextName;
		}
		return name;
	}

	/**
	 * @return the thread name for this Store.
	 */
	public String getThreadName() {
		return threadName;
	}

	/**
	 * @return the name for this Store, used for logging.
	 */
	@Override
	public String getStoreName() {
		return storeName;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Set the Id column for the table.
	 *
	 * @param sessionIdCol the column name
	 */
	public void setSessionIdCol(String sessionIdCol) {
		String oldSessionIdCol = this.sessionIdCol;
		this.sessionIdCol = sessionIdCol;
		support.firePropertyChange("sessionIdCol",
				oldSessionIdCol,
				this.sessionIdCol);
	}

	/**
	 * @return the Id column for the table.
	 */
	public String getSessionIdCol() {
		return this.sessionIdCol;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		String oldDbName = this.dbName;
		this.dbName = dbName;
		support.firePropertyChange("dbName", oldDbName, this.dbName);
	}

	public String getConnectionURL() {
		return connectionURL;
	}

	public void setConnectionURL(String connectionURL) {
		String oldConnString = this.connectionURL;
		this.connectionURL = connectionURL;
		support.firePropertyChange("connectionURL", oldConnString, this.connectionURL);
	}

	public String getSessionCollection() {
		return sessionCollection;
	}

	public void setSessionCollection(String sessionCollection) {
		String oldSessionCollection = this.sessionCollection;
		this.sessionCollection = sessionCollection;
		support.firePropertyChange("sessionCollection", oldSessionCollection, this.sessionCollection);
	}

	public String getSessionAppCol() {
		return sessionAppCol;
	}

	public void setSessionAppCol(String sessionAppCol) {
		String oldSessionAppCol = this.sessionAppCol;
		this.sessionAppCol = sessionAppCol;
		support.firePropertyChange("sessionAppCol", oldSessionAppCol, this.sessionAppCol);
	}

	public String getSessionDataCol() {
		return sessionDataCol;
	}

	public void setSessionDataCol(String sessionDataCol) {
		String oldSessionDataCol = this.sessionDataCol;
		this.sessionDataCol = sessionDataCol;
		support.firePropertyChange("sessionDataCol", oldSessionDataCol, this.sessionDataCol);
	}

	public String getSessionValidCol() {
		return sessionValidCol;
	}

	public void setSessionValidCol(String sessionValidCol) {
		String oldSessionValidCol = this.sessionValidCol;
		this.sessionValidCol = sessionValidCol;
		support.firePropertyChange("sessionValidCol", oldSessionValidCol, this.sessionValidCol);
	}

	public String getSessionMaxInactiveCol() {
		return sessionMaxInactiveCol;
	}

	public void setSessionMaxInactiveCol(String sessionMaxInactiveCol) {
		String oldSessionMaxInactiveCol = this.sessionMaxInactiveCol;
		this.sessionMaxInactiveCol = sessionMaxInactiveCol;
		support.firePropertyChange("sessionMaxInactiveCol", oldSessionMaxInactiveCol, this.sessionMaxInactiveCol);
	}

	public String getSessionLastAccessedCol() {
		return sessionLastAccessedCol;
	}

	public void setSessionLastAccessedCol(String sessionLastAccessedCol) {
		String oldSessionLastAccessedCol = this.sessionLastAccessedCol;
		this.sessionLastAccessedCol = sessionLastAccessedCol;
		support.firePropertyChange("sessionLastAccessedCol", oldSessionLastAccessedCol, this.sessionLastAccessedCol);
	}

	public int getPoolMinSize() {
		return poolMinSize;
	}

	public void setPoolMinSize(int poolMinSize) {
		int oldpoolMinSize = this.poolMinSize;
		this.poolMinSize = poolMinSize;
		support.firePropertyChange("poolMinSize", oldpoolMinSize, this.poolMinSize);
	}

	public int getPoolMaxSize() {
		return poolMaxSize;
	}

	public void setPoolMaxSize(int poolMaxSize) {
		int oldpoolMaxSize = this.poolMaxSize;
		this.poolMaxSize = poolMaxSize;
		support.firePropertyChange("poolMaxSize", oldpoolMaxSize, this.poolMaxSize);
	}

	public int getPoolMaxWaitTime() {
		return poolMaxWaitTime;
	}

	public void setPoolMaxWaitTime(int poolMaxWaitTime) {
		this.poolMaxWaitTime = poolMaxWaitTime;
	}

	public int getPoolMaxConnectionLifeTime() {
		return poolMaxConnectionLifeTime;
	}

	public void setPoolMaxConnectionLifeTime(int poolMaxConnectionLifeTime) {
		this.poolMaxConnectionLifeTime = poolMaxConnectionLifeTime;
	}

	public int getPoolMaxConnectionIdleTime() {
		return poolMaxConnectionIdleTime;
	}

	public void setPoolMaxConnectionIdleTime(int poolMaxConnectionIdleTime) {
		this.poolMaxConnectionIdleTime = poolMaxConnectionIdleTime;
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
	 * Return an integer containing a count of all Sessions
	 * currently saved in this Store.  If there are no Sessions,
	 * <code>0</code> is returned.
	 *
	 * @return the count of all sessions currently saved in this Store
	 *
	 * @exception IOException if an input/output error occurred
	 */
	@Override
	public int getSize() throws IOException {
		int size = 0;

		synchronized (this) {
			int numberOfTries = 2;
			while (numberOfTries > 0) {
				MongoCollection collection = getConnection();

				if (collection == null) {
					return size;
				}

				try {
					Long count = collection.count(eq(sessionAppCol, getName()));
					numberOfTries = 0;
					size = count.intValue();
				} catch (Exception e) {
					getLogger().error(sm.getString(getStoreName() + ".MongoException", e));

				}
				numberOfTries--;
			}
		}
		return size;
	}

	/**
	 * Load the Session associated with the id <code>id</code>.
	 * If no such session is found <code>null</code> is returned.
	 *
	 * @param id a value of type <code>String</code>
	 * @return the stored <code>Session</code>
	 * @exception ClassNotFoundException if an error occurs
	 * @exception IOException if an input/output error occurred
	 */
	@Override
	public Session load(String id) throws ClassNotFoundException, IOException {
		StandardSession _session = null;
		Context context = getManager().getContext();
		Log contextLog = getLogger();

		synchronized (this) {
			int numberOfTries = 2;
			while (numberOfTries > 0) {
				MongoCollection collection = getConnection();
				if (collection == null) {
					return null;
				}

				ClassLoader oldThreadContextCL = context.bind(Globals.IS_SECURITY_ENABLED, null);

				try {

					Collection<Document> docs = collection.find(and(
							eq(sessionAppCol, getName()),
							eq(sessionIdCol, id)
					)).projection(Projections.fields(
							Projections.include(sessionIdCol, sessionDataCol))).into(new ArrayList<Document>());
					Iterator<Document> iter = docs.iterator();
					if (iter.hasNext()) {
						Document doc = iter.next();
						if (doc != null) {
							ObjectId dataId = doc.getObjectId(sessionDataCol);

							try (
									InputStream downloadStream = this.gridFSFilesBucket.openDownloadStream(dataId);
									ObjectInputStream ois =
									     getObjectInputStream(downloadStream)) {

								if (contextLog.isDebugEnabled()) {
									contextLog.debug(sm.getString(
											getStoreName() + ".loading", id, sessionCollection));
								}

								_session = (StandardSession) manager.createEmptySession();
								_session.readObjectData(ois);
								_session.setManager(manager);
							}
						}
					} else if (contextLog.isDebugEnabled()) {
						contextLog.debug(getStoreName() + ": No persisted data object found");
					}
					// Break out after the finally block
					numberOfTries = 0;


				} catch (Exception e) {
					contextLog.error(sm.getString(getStoreName() + ".MongoException", e));

				} finally {
					context.unbind(Globals.IS_SECURITY_ENABLED, oldThreadContextCL);
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
				MongoCollection collection = getConnection();

				if (collection == null) {
					return;
				}

				try {
					remove(id, collection);
					// Break out after the finally block
					numberOfTries = 0;
				} catch (Exception e) {
					getLogger().error(sm.getString(getStoreName() + ".MongoException", e));
				}
				numberOfTries--;
			}
		}

		if (getLogger().isDebugEnabled()) {
			getLogger().debug(sm.getString(getStoreName() + ".removing", id, sessionCollection));
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
				MongoCollection collection = getConnection();
				if (collection == null) {
					return;
				}

				try {
					removeAll(collection);
					// Break out after the finally block
					numberOfTries = 0;
				} catch (Exception e) {
					e.printStackTrace();
					getLogger().error(sm.getString(getStoreName() + ".MongoException", e));
				}
				numberOfTries--;
			}
		}
	}

	/**
	 * Save a session to the Store.
	 *
	 * @param session the session to be stored
	 * @exception IOException if an input/output error occurs
	 */
	@Override
	public void save(Session session) throws IOException {
		ByteArrayOutputStream bos = null;

		synchronized (this) {
			int numberOfTries = 2;
			while (numberOfTries > 0) {
				MongoCollection collection = getConnection();
				if (collection == null) {
					return;
				}

				try {
					// If sessions already exist in DB, remove and insert again.
					// TODO:
					// * Check if ID exists in database and if so use UPDATE.
					remove(session.getIdInternal(), collection);

					bos = new ByteArrayOutputStream();
					try (ObjectOutputStream oos =
							     new ObjectOutputStream(new BufferedOutputStream(bos))) {
						((StandardSession) session).writeObjectData(oos);
					}
					byte[] obs = bos.toByteArray();
					int size = obs.length;
					try (ByteArrayInputStream bis = new ByteArrayInputStream(obs, 0, size);
					     InputStream in = new BufferedInputStream(bis, size)) {

						Document doc = new Document(sessionIdCol, session.getIdInternal())
								.append(sessionAppCol, getName())
								.append(sessionValidCol, session.isValid() ? "1" : "0")
								.append(sessionMaxInactiveCol, session.getMaxInactiveInterval())
								.append(sessionLastAccessedCol, session.getLastAccessedTime());

						ObjectId fileId = this.gridFSFilesBucket.uploadFromStream(session.getIdInternal() + getName(), in);
						doc.append(sessionDataCol, fileId);

						collection.insertOne(doc);
						// Break out after the finally block
						numberOfTries = 0;
					}
				} catch (Exception e) {
					e.printStackTrace();
					getLogger().error(sm.getString(getStoreName() + ".MongoException", e));
				}
				numberOfTries--;
			}
		}

		if (getLogger().isDebugEnabled()) {
			getLogger().debug(sm.getString(getStoreName() + ".saving",
					session.getIdInternal(), sessionCollection));
		}
	}

	/**
	 * Check the connection associated with this store, if it's
	 * <code>null</code> or closed try to reopen it.
	 * Returns <code>null</code> if the connection could not be established.
	 *
	 * @return <code>Connection</code> if the connection succeeded
	 */
	protected MongoCollection getConnection() {

		// Do nothing if there is a database connection already open
		if (collection != null) return collection;

		MongoCollection collection = null;
		if (this.getLogger().isDebugEnabled() ) {
			this.getLogger().debug("Opening mongo collection to " + this.connectionURL + " with " + this.dbName + " and " + this.sessionCollection + " ");
		}

		try {

			MongoClient mongoClient =
					MongoClients.create(
						this.buildClient()
					);

			this.mongoDatabase = mongoClient.getDatabase(this.dbName);
			this.gridFSFilesBucket = GridFSBuckets.create(this.mongoDatabase, "session_data");
			collection = this.mongoDatabase.getCollection(this.sessionCollection);
			this.collection = collection;
		} catch (Exception ex) {
			ex.printStackTrace();
			getLogger().error(sm.getString(getStoreName() + ".checkConnectionMongoException", ex.toString()));
		}

		return collection;
	}



	/**
	 * Close the specified database connection.
	 *
	 *
	 */
	protected void close() {

		// Do nothing if the database connection is already closed
		if (this.mongoDatabase == null)
			return;

		this.mongoDatabase = null;
		this.collection = null;

	}

	/**
	 * Release the connection, if it
	 * is associated with a connection pool.
	 *
	 * @param conn The connection to be released
	 */
	protected void release(Connection conn) {
		this.close();

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

		this.collection = this.getConnection();
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


	private MongoClientSettings buildClient() {
		MongoClientSettings expected = MongoClientSettings.builder()
				.credential(
						MongoCredential.createCredential(
								this.username, this.dbName, this.password.toCharArray()))

				.applyConnectionString(new ConnectionString(this.connectionURL))

				.applyToConnectionPoolSettings(new Block<ConnectionPoolSettings.Builder>() {
					@Override
					public void apply(final ConnectionPoolSettings.Builder builder) {
						builder.minSize(poolMinSize)
								.maxSize(poolMaxSize)
								.maxWaitQueueSize(10 * 7) // maxPoolSize * waitQueueMultiple
								.maxWaitTime(poolMaxWaitTime, TimeUnit.MILLISECONDS)
								.maxConnectionLifeTime(poolMaxConnectionLifeTime, TimeUnit.MILLISECONDS)
								.maxConnectionIdleTime(poolMaxConnectionIdleTime, TimeUnit.MILLISECONDS);
					}
				})
				.applyToServerSettings(new Block<ServerSettings.Builder>() {
					@Override
					public void apply(final ServerSettings.Builder builder) {
						builder.heartbeatFrequency(20000, TimeUnit.MILLISECONDS);
					}
				})
				.applyToSocketSettings(new Block<SocketSettings.Builder>() {
					@Override
					public void apply(final SocketSettings.Builder builder) {
						builder.connectTimeout(2500, TimeUnit.MILLISECONDS)
								.readTimeout(5500, TimeUnit.MILLISECONDS);
					}
				})
				/*.applyToSslSettings(new Block<SslSettings.Builder>() {
					@Override
					public void apply(final SslSettings.Builder builder) {
						builder.enabled(true)
								.invalidHostNameAllowed(true);
					}
				})*/
				.readConcern(ReadConcern.MAJORITY)
				.readPreference(ReadPreference.secondary())
				.writeConcern(WriteConcern.MAJORITY.withWTimeout(2500, TimeUnit.MILLISECONDS))
				.applicationName(this.getName())
				.compressorList(Arrays.asList(MongoCompressor.createZlibCompressor().withProperty(MongoCompressor.LEVEL, 5)))
                .retryWrites(true)
				.build();
		return expected;
	}


	/**
	 * Return an array containing the session identifiers of all Sessions
	 * currently saved in this Store.  If there are no such Sessions, a
	 * zero-length array is returned.
	 *
	 * @param expiredOnly flag, whether only keys of expired sessions should
	 *        be returned
	 * @return array containing the list of session IDs
	 *
	 * @exception IOException if an input/output error occurred
	 */
	private String[] keys(boolean expiredOnly) throws IOException {
		String keys[] = null;
		synchronized (this) {
			int numberOfTries = 2;
			while (numberOfTries > 0) {

				MongoCollection collection = getConnection();
				if (collection == null) {
					return new String[0];
				}

				FindIterable iter = null;
				if (!expiredOnly) {
					iter = collection.find(
							and(eq(sessionAppCol, getName()))
					);
				} else {
					iter = collection.find(
							and(
									eq(sessionAppCol, getName()),
									where("this." + sessionLastAccessedCol + " + this." + sessionMaxInactiveCol + " * 1000 < " + System.currentTimeMillis())
							)
					);
				}
				try {

					Collection<Document> ids = iter.projection(
							Projections.fields(Projections.include(sessionIdCol))
					).into(new ArrayList<Document>());

					List<String> tmpkeys = new ArrayList<>();
					for (Document doc : ids) {
						tmpkeys.add(doc.getString(sessionIdCol));
					}
					keys = tmpkeys.toArray(new String[tmpkeys.size()]);
					numberOfTries = 0;

				} catch (Exception e) {
					getLogger().error(sm.getString(getStoreName() + ".MongoException", e));
					keys = new String[0];
				}
				numberOfTries--;


			}
			return keys;
		}
	}

	/**
	 * Remove the Session with the specified session identifier from
	 * this Store, if present.  If no such Session is present, this method
	 * takes no action.
	 *
	 * @param id Session identifier of the Session to be removed
	 * @param collection open connection to be used
	 * @throws Exception if an error occurs while talking to the database
	 */
	private void remove(String id, MongoCollection collection) throws Exception {
		if (getLogger().isDebugEnabled()) {
			getLogger().debug("Removing session id " + id + " for app " + getName());
		}
		Document doc = (Document)collection.find(and(
				eq(sessionIdCol, id),
				eq(sessionAppCol, getName())
		)).first();
		if (doc != null ) {
			ObjectId dataId = doc.getObjectId(sessionDataCol);
			if (getLogger().isDebugEnabled()) {
				getLogger().debug("Got session data id " + dataId.toString());
			}
			this.gridFSFilesBucket.delete(dataId);
		}

		collection.deleteMany(and(
				eq(sessionAppCol, getName()),
				eq(sessionIdCol, id)
		));

	}


	private void removeAll(MongoCollection collection) throws Exception {
		if (getLogger().isDebugEnabled()) {
			getLogger().debug("Removing all sessions for app " + getName());
		}

		this.gridFSFilesBucket.drop();

		collection.deleteMany(and(
				eq(sessionAppCol, getName())
		));
	}


}
