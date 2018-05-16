package com.pearson.tomcat.session

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters
import org.apache.catalina.Context
import org.apache.catalina.Manager
import org.apache.catalina.session.StandardSession
import org.apache.juli.logging.Log
import org.bson.Document
import spock.lang.Specification

class MongoStoreSpec extends Specification {

	//static final URL = "mongodb://localhost:27017"
	//static final DB_NAME = "tomcatsessionsdb"
	//static final USER = "tomcat_sessions_user"
	//static final PWD = "password1!"

	static final URL = "mongodb+srv://qidevsessioncluster-kyjjk.mongodb.net/"
	static final DB_NAME = "tomcatsessionsdb"
	static final USER = "sessions_user"
	static final PWD = "password1!"

	MongoStore mongoStore
	def manager
	def context
	MongoCollection collection

	def setup() {
		manager = Mock(Manager)
		manager.createEmptySession() >> {
			return new StandardSession(manager)
		}
		manager.willAttributeDistribute(_,_) >> { String name, Object value ->
			true
		}
		context = Mock(Context)
		context.getLogger() >> {
			Mock(Log)
		}
		manager.getContext() >> {
			context
		}
		mongoStore = new MongoStore(connectionURL: URL, username: USER, password: PWD, dbName: DB_NAME,
				manager: manager, name: 'choose-share')
		collection = mongoStore.getConnection()
		clearAll()
	}


	def "should get collection and grid fs" () {
		expect:
		collection.namespace.fullName == "tomcatsessionsdb.qi_sessions"
		mongoStore.gridFSFilesBucket.bucketName == "session_data"

	}

	def "should get all keys"() {
		given:
		mongoStore.sessionIdCol = 'mySessionIdCol'
		mongoStore.sessionAppCol = 'appCol'
		[
		        [
		                id: 'ID0001',
						app: 'choose-share'
		        ],
				[
						id: 'ID002',
						app: 'someotgher-app'
				],
				[
				        id: 'ID003',
						app: 'choose-share'
				]
		].each {
			collection.insertOne(new Document('mySessionIdCol', it.id).append('appCol', it.app))
		}

		when:
		def keys = mongoStore.keys()

		then:
		keys == ["ID0001", "ID003"]

		cleanup:
		clearAll()
	}

	def "should get only expired keys"() {
		given:
		def now = System.currentTimeMillis()
		[
				[
						id: 'ID0001',
						app: 'choose-share',
						lastAccessed: now - (10*1000)
				],

				[
						id: 'ID003',
						app: 'choose-share',
						lastAccessed: now - (30*1000)
				]
		].each {
			collection.insertOne(new Document(mongoStore.sessionIdCol, it.id).append(mongoStore.sessionAppCol, it.app)
			.append(mongoStore.sessionMaxInactiveCol, 30).append(mongoStore.sessionLastAccessedCol, it.lastAccessed))

		}

		when:
		def keys = mongoStore.expiredKeys()

		then:
		keys == ["ID003"]

		cleanup:
		clearAll()
	}

	def "should get size"() {
		given:
		mongoStore.sessionIdCol = 'mySessionIdCol'
		mongoStore.sessionAppCol = 'appCol'
		[
				[
						id: 'ID0001',
						app: 'choose-share'
				],
				[
						id: 'ID002',
						app: 'someotgher-app'
				],
				[
						id: 'ID003',
						app: 'choose-share'
				]
		].each {
			collection.insertOne(new Document('mySessionIdCol', it.id).append('appCol', it.app))
		}

		when:
		def size = mongoStore.getSize()

		then:
		size == 2

		cleanup:
		clearAll()
	}

	def "should save and restore session"() {
		given:
		def myObject = new ASerializableDTO(name: 'Child', salary: 9000, parent: new ASerializableDTO(name: 'parent', salary: 10000))
		def session = new StandardSession(manager)

		session.creationTime = System.currentTimeMillis() - 100
		session.maxInactiveInterval = 30
		session.valid = true
		session.setAttribute("mySession", "SOMEVALUE")
		session.setAttribute("myObject", myObject)
		session.setId("MYSESSIONID", false)

		when:
		mongoStore.save(session)
		def retrievedSession = mongoStore.load("MYSESSIONID")

		then:
		retrievedSession.id == "MYSESSIONID"
		retrievedSession.getAttribute('mySession') == "SOMEVALUE"
		retrievedSession.getAttribute('myObject').toString() == myObject.toString()
	}


	private clearAll() {
		mongoStore.clear()
		collection.deleteMany(Filters.and(
				Filters.ne(mongoStore.sessionIdCol, 'none')
		))
	}


}
