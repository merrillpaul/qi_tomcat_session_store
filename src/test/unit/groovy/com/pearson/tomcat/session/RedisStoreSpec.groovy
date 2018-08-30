package com.pearson.tomcat.session

import org.apache.catalina.Context
import org.apache.catalina.Manager
import org.apache.catalina.session.StandardSession
import org.apache.juli.logging.Log
import redis.clients.jedis.Jedis
import spock.lang.Specification

import java.beans.PropertyChangeEvent
import java.beans.PropertyChangeListener

class RedisStoreSpec extends Specification {
	def static Process p
	RedisPooledStore store
	def oldVal, newVal
	def manager, context
	Jedis testJedis

	def setupSpec() {
		println "Start Redis"
		p = Runtime.getRuntime().exec("redis-server")

	}

	def cleanupSpec() {
		println "After Redis"
		p?.destroyForcibly()
	}


	def setup() {
		manager = Mock(Manager)
		manager.createEmptySession() >> {
			return new StandardSession(manager)
		}
		manager.willAttributeDistribute(_, _) >> { String name, Object value ->
			true
		}
		context = Mock(Context)
		context.getLogger() >> {
			Mock(Log)
		}
		manager.getContext() >> {
			context
		}
		store = new RedisPooledStore(manager: manager)
		testJedis = store.getResource()
	}

	def cleanup() {
		testJedis.flushDB()
		store?.close()
	}

	def "should have store names setup"() {
		expect:
		store.storeName == "RedisPooledStore"
		store.threadName == "RedisPooledStore"
	}

	def "should fire property changes for set keyname"() {
		given:
		addListener(store.support, "keyName")

		when:
		store.keyName = 'newKeyname'

		then:
		newVal == "newKeyname"
		oldVal == "qi_sessions"
	}

	def "should fire property changes for set host name"() {
		given:
		addListener(store.support, "redisHost")

		when:
		store.redisHost = 'dubdub.com'

		then:
		newVal == "dubdub.com"
		oldVal == "localhost"
	}

	def "should fire property changes for set port"() {
		given:
		addListener(store.support, "redisPort")

		when:
		store.redisPort = 1000

		then:
		newVal == 1000
		oldVal == 6379
	}

	def "should fire property changes for set maxTotal"() {
		given:
		addListener(store.support, "maxTotal")

		when:
		store.maxTotal = 23

		then:
		newVal == 23
		oldVal == 128
	}

	def "should fire property changes for set maxIdle"() {
		given:
		addListener(store.support, "maxIdle")

		when:
		store.maxIdle = 23

		then:
		newVal == 23
		oldVal == 128
	}

	def "should fire property changes for set minIdle"() {
		given:
		addListener(store.support, "minIdle")

		when:
		store.minIdle = 10

		then:
		newVal == 10
		oldVal == 5
	}

	def "should fire property changes for set testOnborrow"() {
		given:
		addListener(store.support, "testOnBorrow")

		when:
		store.testOnBorrow = false

		then:
		!newVal
		oldVal
	}

	def "should fire property changes for set testOnReturn"() {
		given:
		addListener(store.support, "testOnReturn")

		when:
		store.testOnReturn = false

		then:
		!newVal
		oldVal
	}

	def "should save and restore session"() {
		given:
		def myObject = new ASerializableDTO(name: 'Child', salary: 9000, parent: new ASerializableDTO(name: 'parent', salary: 10000))
		def session = new StandardSession(manager)

		session.creationTime = System.currentTimeMillis() - 100
		session.access()
		session.maxInactiveInterval = 30
		session.valid = true
		session.setAttribute("mySession", "SOMEVALUE")
		session.setAttribute("myObject", myObject)
		session.setId("MYSESSIONID", false)

		when:
		store.save(session)
		def retrievedSession = store.load("MYSESSIONID")

		then:
		retrievedSession.id == "MYSESSIONID"
		retrievedSession.lastAccessedTime == session.lastAccessedTimeInternal
		retrievedSession.getAttribute('mySession') == "SOMEVALUE"
		retrievedSession.getAttribute('myObject').toString() == myObject.toString()
	}

	def "should return null if invalid id is used to load a session"() {
		when:
		def retrievedSession = store.load("INVALID_ID")

		then:
		!retrievedSession
	}

	def "should over write session and related information when saved again"() {
		given:
		def myObject = new ASerializableDTO(name: 'Child', salary: 9000, parent: new ASerializableDTO(name: 'parent', salary: 10000))
		def session = new StandardSession(manager)
		def jedis = store.getResource()
		session.creationTime = System.currentTimeMillis() - 100
		session.access()
		session.maxInactiveInterval = 30
		session.valid = true
		session.setAttribute("mySession", "SOMEVALUE")
		session.setAttribute("myObject", myObject)
		session.setId("MYSESSIONID", false)
		store.save(session)
		def firstRetrievedSession = store.load("MYSESSIONID")

		when:
		firstRetrievedSession.access()
		assert firstRetrievedSession.getAttribute('mySession') == "SOMEVALUE"
		assert firstRetrievedSession.getAttribute('myObject').name == 'Child'

		firstRetrievedSession.removeAttribute("mySession")
		firstRetrievedSession.setAttribute("secondData", "data")
		myObject.name = "NewName"
		firstRetrievedSession.setAttribute("myObject", myObject)
		assert jedis.zrange("qi_sessions_expiry", 0, -1).collect { it } == ["MYSESSIONID"]
		assert jedis.zrangeWithScores("qi_sessions_expiry", 0, -1).score[0].longValue() ==
				firstRetrievedSession.lastAccessedTime + (firstRetrievedSession.maxInactiveInterval * 1000)
		store.save(firstRetrievedSession)
		def secondRetrievedSession = store.load("MYSESSIONID")

		then:
		secondRetrievedSession.id == "MYSESSIONID"
		firstRetrievedSession.id == secondRetrievedSession.id
		secondRetrievedSession.getAttribute('secondData') == "data"
		secondRetrievedSession.getAttribute('mySession') == null
		secondRetrievedSession.getAttribute('myObject').name == 'NewName'
		// checks the zadd index value for the new one
		jedis.zrangeWithScores("qi_sessions_expiry", 0, -1).score[0].longValue() ==
				secondRetrievedSession.lastAccessedTime + (secondRetrievedSession.maxInactiveInterval * 1000)

		cleanup:
		jedis?.close()
	}

	def "should get size accurately"() {
		given:
		(0..4).each {
			def session = new StandardSession(manager)
			session.creationTime = System.currentTimeMillis() - 100
			session.access()
			session.maxInactiveInterval = 30
			session.valid = true
			session.setAttribute("mySession", "SOMEVALUE${it}")
			session.setId("MYSESSIONID${it}", false)
			store.save(session)
		}

		when:
		def size = store.size

		then:
		size == 5
	}

	def "should get expired keys"() {
		given:
		def now = System.currentTimeMillis()
		[
				[
						time : now - (1000 * 31),
						intvl: 30
				],
				[
						time : now,
						intvl: 10
				],
				[
						time : now - (1000 * 31),
						intvl: 10
				],
				[
						time : now,
						intvl: 30
				]
		].eachWithIndex { def entry, int i ->
			def session = new StandardSession(manager)
			session.creationTime = entry.time
			session.maxInactiveInterval = entry.intvl
			session.valid = true
			session.setId("MYSESSIONID${i}", false)
			store.save(session)
		}

		when:
		def ids = store.expiredKeys()

		then:
		ids.sort() == ["MYSESSIONID0", "MYSESSIONID2"]
	}


	def "should get all keys"() {
		given:
		def now = System.currentTimeMillis()
		[
				[
						time : now - (1000 * 31),
						intvl: 30
				],
				[
						time : now,
						intvl: 10
				],
				[
						time : now - (1000 * 31),
						intvl: 10
				],
				[
						time : now,
						intvl: 30
				]
		].eachWithIndex { def entry, int i ->
			def session = new StandardSession(manager)
			session.creationTime = entry.time
			session.maxInactiveInterval = entry.intvl
			session.valid = true
			session.setId("MYSESSIONID${i}", false)
			store.save(session)
		}

		when:
		def ids = store.keys()

		then:
		ids.sort() == ["MYSESSIONID0", "MYSESSIONID1", "MYSESSIONID2", "MYSESSIONID3"]
	}

	private addListener(support, propName) {
		def listener = Mock(PropertyChangeListener)

		listener.propertyChange(_) >> { PropertyChangeEvent event ->
			newVal = event.newValue
			oldVal = event.oldValue
		}
		support.addPropertyChangeListener(propName, listener)
	}
}
