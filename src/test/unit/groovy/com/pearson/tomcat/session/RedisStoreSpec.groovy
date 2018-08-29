package com.pearson.tomcat.session

import spock.lang.Specification

class RedisStoreSpec extends Specification {
	def static Process p

	def setupSpec() {
		println "Start Redis"
		p = Runtime.getRuntime().exec("redis-server")

	}

	def cleanupSpec() {
		println "After Redis"
		p?.destroyForcibly()
	}
}
