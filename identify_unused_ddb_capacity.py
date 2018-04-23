from __future__ import print_function
import logging
import boto3
from datetime import datetime, timedelta
import collections


logger = logging.getLogger()
PROVISIONED_READ_CAPACITY_UNITS = 'ProvisionedReadCapacityUnits'
CONSUMED_READ_CAPACITY_UNITS = 'ConsumedReadCapacityUnits'
METRICS_STATISTIC_SUM = 'Sum'

def parseDDBTableList(inputJson):
    '''
        Takes in DDB list tables json and return list of table names
    '''
    tableNames = set()

    if not inputJson:
        raise Exception("cannot parse empty input")

    if not inputJson['TableNames']:
        raise Exception("malformed input, does not contain table names {0}".format(inputJson))

    for tn in inputJson['TableNames']:
        tableNames.add(tn)

    return tableNames

def getDDBMetricsStatistics(tableName, metricName, region, startTime, endTime, period, statistic, unit):
    '''
        Returns cloud watch metrics for Dynamodb
        Input(s):
            tableName : Name of ddb table
            metricName : Example ProvisionedReadCapacityUnits
            region : Example us-east-1
            startTime : format 2017-11-10T12:00:00
            endTime : format 2017-11-20T12:00:00
            period : in mins Example 60 for one hour
            statistic : 'SampleCount'|'Average'|'Sum'|'Minimum'|'Maximum'
            unit :  [ Seconds, Microseconds, Milliseconds, Bytes, Kilobytes, Megabytes, Gigabytes, Terabytes, Bits, Kilobits, Megabits,
                    Gigabits, Terabits, Percent, Count, Bytes/Second, Kilobytes/Second, Megabytes/Second, Gigabytes/Second, Terabytes/Second,
                    Bits/Second, Kilobits/Second, Megabits/Second, Gigabits/Second, Terabits/Second, Count/Second, None ]
        Returns:

    '''
    client = boto3.client('cloudwatch')

    resp = client.get_metric_statistics(Namespace='AWS/DynamoDB', MetricName=metricName, StartTime=startTime, EndTime=endTime, Period=period,
        Statistics=[statistic], Dimensions=[{'Name':'TableName','Value':tableName}], Unit=unit)

    return resp

def getDDBTableGlobalSecondaryIndexNames(tableName, region):
    client = boto3.client('dynamodb',region_name=region)

    resp = client.describe_table(
        TableName=tableName
    )

    if not resp or not resp['Table']:
        raise Exception('Could not retrieve describe table for {0} and region {1}'.format(tableName, region))

    #print(resp['Table']['LocalSecondaryIndexes'])

    indexNames = list()
    if 'GlobalSecondaryIndexes' in resp['Table'].keys():
        for lsi in resp['Table']['GlobalSecondaryIndexes']:
            indexNames.append(lsi['IndexName'])

    return indexNames


def getDDBTableProvisionedCapacity(tableName, region, capacityType, indexName=None):
    client = boto3.client('dynamodb',region_name=region)

    resp = client.describe_table(
        TableName=tableName
    )

    if not resp or 'ProvisionedThroughput' not in resp['Table'].keys():
        raise Exception('Could not retrieve describe table for {0} and region {1}'.format(tableName, region))

    if not indexName:
        if capacityType == 'read':
            return resp['Table']['ProvisionedThroughput']['ReadCapacityUnits']
        elif capacityType == 'write':
            return resp['Table']['ProvisionedThroughput']['WriteCapacityUnits']
        else:
            raise Exception('Unsupported capacityType')
    else:
        if 'GlobalSecondaryIndexes' not in resp['Table'].keys():
            raise Exception('Cannot find capacity for table {0} and index {1} in region {2}'.format(tableName, indexName, region))
        else:
            for ind in resp['Table']['GlobalSecondaryIndexes']:
                if ind['IndexName'] == indexName:
                    if capacityType == 'read':
                        return ind['ProvisionedThroughput']['ReadCapacityUnits']
                    elif capacityType == 'write':
                        return ind['ProvisionedThroughput']['WriteCapacityUnits']
                    else:
                        raise Exception('Unsupported capacityType')


def getMetricsDataPoints(inputJson):
    provisionedDDBCapacity = dict()
    for dp in inputJson:
        provisionedDDBCapacity[str(dp['Timestamp'])] = dp['Sum']
    return provisionedDDBCapacity

def determineUnusedDDBCapacity(region, tableName, indexName=None):
    endTime = datetime.now()
    startTime = datetime.now()-timedelta(days=5)

    readCapacityUnits = None
    writeCapacityUnits = None
    if not indexName:
        readCapacityUnits = getDDBTableProvisionedCapacity(tableName, region, 'read')
        writeCapacityUnits = getDDBTableProvisionedCapacity(tableName, region, 'write')
        print('Provisioned Read Capacity Unit for table {0} is {1}'.format(tableName, readCapacityUnits))
        print('Provisioned Write Capacity Unit for table {0} is {1}'.format(tableName, writeCapacityUnits))
    else:
        readCapacityUnits = getDDBTableProvisionedCapacity(tableName, region, 'read', indexName)
        writeCapacityUnits = getDDBTableProvisionedCapacity(tableName, region, 'write', indexName)
        print('Provisioned Read Capacity Unit for table {0} and index {1} is {2}'.format(tableName, indexName, readCapacityUnits))
        print('Provisioned Write Capacity Unit for table {0} and index {1} is {2}'.format(tableName, indexName, writeCapacityUnits))

    consumedReadCapacityMetricsData = getDDBMetricsStatistics(tableName, CONSUMED_READ_CAPACITY_UNITS, region, startTime, endTime, 300, 'Sum', 'Count')

    if not consumedReadCapacityMetricsData['Datapoints']:
        print("Table {0} has max consumed capacity of 0% ".format(tableName))
    else:
        consumedDDBCapacity = getMetricsDataPoints(consumedReadCapacityMetricsData['Datapoints'])

        maxUsed = 0.0
        for ts, uc in consumedDDBCapacity.iteritems():
            consumedCapacity = float(uc)/float(readCapacityUnits)*100
            if  consumedCapacity > maxUsed:
                maxUsed = consumedCapacity

        print("Table {0} has max consumed capacity of {1}% ".format(tableName, maxUsed))

def identifyUnusedDDBCapacity(region, tableToCheck=None):
    client = boto3.client('dynamodb',region_name=region)
    #print(client.list_tables());
    resp = client.list_tables()
    tableNames = parseDDBTableList(resp)

    print("Table Names for Region {0} are {1}".format(region, tableNames))

    for tableName in tableNames:

        if tableToCheck and tableToCheck != tableName:
            continue

        #first check table capacity
        determineUnusedDDBCapacity(region, tableName)

        #identify indexes and check their capacity
        tableIndexes = getDDBTableGlobalSecondaryIndexNames(tableName, region)

        if not tableIndexes:
            print('No LSI or GSI found for table {0}'.format(tableName))

        for ti in tableIndexes:
            determineUnusedDDBCapacity(region, tableName, ti)


if __name__ == "__main__":
    #identifyUnusedDDBCapacity('us-east-1', 'bittrex-median-volumes')
    identifyUnusedDDBCapacity('us-east-1')
