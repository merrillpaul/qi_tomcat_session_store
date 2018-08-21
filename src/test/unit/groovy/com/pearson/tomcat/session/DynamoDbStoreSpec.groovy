package com.pearson.tomcat.session

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex
import com.amazonaws.services.dynamodbv2.model.Projection
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput
import com.amazonaws.services.dynamodbv2.util.TableUtils
import groovy.json.JsonSlurper
import spock.lang.Specification

class DynamoDbStoreSpec extends Specification {

	def static DYNAMODB_PROCESS = "java " +
			"-Djava.library.path=${System.getProperty("user.dir")}/tools/dynamodb/DynamoDBLocal_lib/ -jar " +
			"${System.getProperty("user.dir")}/tools/dynamodb/DynamoDBLocal.jar -sharedDb -port 8888"
	def static Process p
	def setupSpec() {
		String line
		println "Start DynamoTest"
		p = Runtime.getRuntime().exec(DYNAMODB_PROCESS)
		/*BufferedReader input =
				new BufferedReader
						(new InputStreamReader(p.getInputStream()))
		while ((line = input.readLine()) != null) {
			System.out.println(line)
		}
		input.close()*/
		createTable()
	}

	def cleanupSpec() {
		println "After DynamoTest"
		p?.destroyForcibly()
	}

	def "test"() {
		expect:
		1 == 1
	}



	def static createTable() {
		def ddl = new File("${System.getProperty("user.dir")}/src/db/sessions_table.json").text
		def json = new JsonSlurper().parseText(ddl)
		def amazonDb = prepareDynamoDb()

		def attributeDefinitions = json.AttributeDefinitions.collect {
			new AttributeDefinition(attributeType: it.AttributeType, attributeName: it.AttributeName)
		}
		def keySchema = json.KeySchema.collect {
			new KeySchemaElement(keyType: it.KeyType, attributeName: it.AttributeName)
		}
		def localIndices = json.GlobalSecondaryIndexes.collect {
			new GlobalSecondaryIndex(
					indexName: it.IndexName,
					projection: new Projection(projectionType: it.Projection.ProjectionType),
					keySchema: it.KeySchema.collect {
						new KeySchemaElement(keyType: it.KeyType, attributeName: it.AttributeName)
					},
					provisionedThroughput: new ProvisionedThroughput(
					readCapacityUnits: it.ProvisionedThroughput.ReadCapacityUnits,
					writeCapacityUnits: it.ProvisionedThroughput.WriteCapacityUnits
			))

		}
		def createTable = new CreateTableRequest(tableName: json.TableName,
				attributeDefinitions: attributeDefinitions, keySchema: keySchema,
				globalSecondaryIndexes: localIndices,
				provisionedThroughput: new ProvisionedThroughput(
						readCapacityUnits: json.ProvisionedThroughput.ReadCapacityUnits,
						writeCapacityUnits: json.ProvisionedThroughput.WriteCapacityUnits
				))
		TableUtils.deleteTableIfExists(amazonDb, new DeleteTableRequest(json.TableName))
		def created = TableUtils.createTableIfNotExists(amazonDb, createTable)
		TableUtils.waitUntilActive(amazonDb, createTable.tableName)
		if (created) {
			println "Created dynamo table ${createTable.tableName}"
		}
	}


	def static prepareDynamoDb() {

		def awsCredentials = new BasicAWSCredentials('xxxx', 'xxxxxx')
		AmazonDynamoDBClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
		.withEndpointConfiguration(
				new AwsClientBuilder.EndpointConfiguration("http://localhost:8888", "us-west-1")
		).build()
	}
}
