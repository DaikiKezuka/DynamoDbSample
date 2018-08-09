using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace DynamoDbSample
{
    class Program
    {
        public static void Main(string[] args)
        {
            var client = BuildClient_local();

            var res = CreateSampleTable(client);
            Console.WriteLine(res);
            
            SaveItem(client);

            LoadItem(client);

            LoadItemUsingDocument(client);

            DeleteItemUsingDocument(client);

            Console.WriteLine("App is finished!");
        }

        private static GetItemOperationConfig CreateItemConfig()
        {
            return new GetItemOperationConfig
            {
                AttributesToGet = CreateJobKeyList(),
                ConsistentRead = true
            };
        }

        private static AmazonDynamoDBClient BuildClient_local()
        {
            var clientConfig = new AmazonDynamoDBConfig
            {
                ServiceURL = "http://localhost:8000"
            };

            AmazonDynamoDBClient client = new AmazonDynamoDBClient(clientConfig);

            return client;
        }

        #region CreateTable
        private static TableDescription CreateSampleTable(AmazonDynamoDBClient client)
        {
            var request = CreateTableRequest();

            try
            {
                var result = client.CreateTable(request);
                Console.WriteLine("Table created.");
                return result.TableDescription;
            }
            catch (ResourceInUseException e)
            {
                Console.WriteLine("Table already exist. Fetching description...StatusCode is {0}.", e.StatusCode);
                var description = client.DescribeTable(new DescribeTableRequest {TableName = TargetTableName});
                return description.Table;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        private static CreateTableRequest CreateTableRequest()
        {
            return new CreateTableRequest
            {
                TableName = TargetTableName,
                AttributeDefinitions = new List<AttributeDefinition>
                {
                    new AttributeDefinition {AttributeName = JobKey.BatchId, AttributeType = "S"}
                },
                KeySchema = new List<KeySchemaElement>
                {
                    new KeySchemaElement
                    {
                        AttributeName = JobKey.BatchId,
                        KeyType = KeyType.HASH
                    }
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 10,
                    WriteCapacityUnits = 5
                }
            };
        }

        #endregion


        const string TargetTableName = "Job";

        private static string _requestBatchId;

        private static void SaveItem(AmazonDynamoDBClient client)
        {
            _requestBatchId = Guid.NewGuid().ToString("N");
            client.PutItem(
                TargetTableName,
                new Dictionary<string, AttributeValue>
                {
                    {JobKey.BatchId, new AttributeValue {S = _requestBatchId}},
                    {JobKey.JobId, new AttributeValue {S = $"This is {JobKey.JobId}."}},
                    {JobKey.UserId, new AttributeValue {S = $"This is {JobKey.UserId}."}},
                    {JobKey.Priority, new AttributeValue {S = $"This is {JobKey.Priority}."}},
                    {JobKey.Status, new AttributeValue {S = $"This is {JobKey.Status}."}},
                    {JobKey.Source, new AttributeValue {S = $"This is {JobKey.Source}."}},
                    {JobKey.EcsTaskId, new AttributeValue {S = $"This is {JobKey.EcsTaskId}."}},
                    {JobKey.ErrorStatus, new AttributeValue {S = $"This is {JobKey.ErrorStatus}."}},
                    {JobKey.StartTime, new AttributeValue {S = $"This is {JobKey.StartTime}."}},
                    {JobKey.EndTime, new AttributeValue {S = $"This is {JobKey.EndTime}."}},
                    {JobKey.Params, new AttributeValue {S = $"This is {JobKey.Params}."}},
                    {JobKey.ResultPath, new AttributeValue {S = $"This is {JobKey.ResultPath}."}},
                    {JobKey.RetriesCount, new AttributeValue {S = $"This is {JobKey.RetriesCount}."}},
                    {JobKey.Ttl, new AttributeValue {S = $"This is {JobKey.Ttl}."}}
                }
            );
            Console.WriteLine("Finished put item.");
        }

        private static void LoadItem(AmazonDynamoDBClient client)
        {
            var response = client.GetItem(
                TargetTableName,
                new Dictionary<string, AttributeValue>
                {
                    {JobKey.BatchId, new AttributeValue {S = _requestBatchId}}
                }
            );

            foreach (var v in response.Item)
            {
                Console.WriteLine($"Key: {v.Key}");
                Console.WriteLine($"Value: {v.Value.S}{v.Value.N}");
            }
            
            Console.ReadKey();
        }

        private static void LoadItemUsingDocument(AmazonDynamoDBClient client)
        {
            var job = Table.LoadTable(client, TargetTableName);

            var doc = job.GetItem(_requestBatchId, CreateItemConfig());

            foreach (var v in doc.Keys)
            {
                Console.WriteLine($"Key: {v}");
                Console.WriteLine($"Value: {doc[v]}");
            }
            Console.ReadKey();
        }

        private static void DeleteItemUsingDocument(AmazonDynamoDBClient client)
        {
            var job = Table.LoadTable(client, TargetTableName);

            var doc = job.GetItem(_requestBatchId, CreateItemConfig());

            job.DeleteItem(doc);
        }

        public class JobKey
        {
            public const string BatchId = "BatchId";
            public const string JobId = "JobId";
            public const string UserId = "UserId";
            public const string Priority = "Priority";
            public const string Status = "Status";
            public const string Source = "Source"; // 登録元
            public const string EcsTaskId = "EcsTaskId"; // ECS Task Id
            public const string ErrorStatus = "ErrorStatus";
            public const string StartTime = "StartTime"; // 開始時間
            public const string EndTime = "EndTime"; // 終了時間
            public const string Params = "Params"; // 入力パラメーター
            public const string ResultPath = "ResultPath"; // 実行結果
            public const string RetriesCount = "RetriesCount"; // リトライ回数
            public const string Ttl = "Ttl"; // TTL（DynamoDBの生存期間）
        }

        private static List<string> CreateJobKeyList()
        {
            return new List<string>
            {
                JobKey.BatchId,
                JobKey.JobId,
                JobKey.UserId,
                JobKey.Priority,
                JobKey.Status,
                JobKey.Source,
                JobKey.EcsTaskId,
                JobKey.ErrorStatus,
                JobKey.StartTime,
                JobKey.EndTime,
                JobKey.Params,
                JobKey.ResultPath,
                JobKey.RetriesCount,
                JobKey.Ttl,
            };
        }
    }
}