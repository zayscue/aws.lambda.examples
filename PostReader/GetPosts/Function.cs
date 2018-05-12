using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace GetPosts
{
    public class Function
    {
        private readonly IAmazonDynamoDB _dbClient;

        /// <summary>
        /// Default constructor that Lambda will invoke.
        /// </summary>
        public Function()
        {
            _dbClient = new AmazonDynamoDBClient();
        }

        /// <summary>
        /// Initializes a new instance with all the required
        /// dependencies for testing purposes.
        /// </summary>
        /// <param name="dbClient"></param>
        public Function(IAmazonDynamoDB dbClient)
        {
            _dbClient = dbClient ?? throw new ArgumentNullException(nameof(dbClient));
        }


        /// <summary>
        ///
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<IEnumerable<Post>> FunctionHandler(Request input, ILambdaContext context)
        {
            var logger = context.Logger;

            var id = input?.postId != null 
                ? input.postId : "*";
            logger.LogLine(id);

            //Retrieve data from DynamoDB
            var tableName = Environment.GetEnvironmentVariable("DB_TABLE_NAME");
            var posts = new List<Post>();
            if(string.Equals(id, "*"))
            {
                var queryResults = await _dbClient.ScanAsync(new ScanRequest
                {
                    TableName = tableName
                });
                foreach(var item in queryResults.Items)
                {
                    var newPost = new Post();
                    if (item.ContainsKey("id"))
                        newPost.id = item["id"].S;
                    if (item.ContainsKey("text"))
                        newPost.text = item["text"].S;
                    if (item.ContainsKey("voice"))
                        newPost.voice = item["voice"].S;
                    if (item.ContainsKey("status"))
                        newPost.status = item["status"].S;
                    if (item.ContainsKey("url"))
                        newPost.url = item["url"].S;
                    posts.Add(newPost);
                }
            }
            else
            {
                var queryResults = await _dbClient.QueryAsync(new QueryRequest
                {
                    TableName = tableName,
                    KeyConditions = new Dictionary<string, Condition>
                    {
                        {
                            "id", new Condition()
                            {
                                ComparisonOperator = ComparisonOperator.EQ,
                                AttributeValueList = new List<AttributeValue>
                                {
                                    new AttributeValue(id)
                                }
                            }
                        }
                    }
                });
                foreach (var item in queryResults.Items)
                {
                    var newPost = new Post();
                    if (item.ContainsKey("id"))
                        newPost.id = item["id"].S;
                    if (item.ContainsKey("text"))
                        newPost.text = item["text"].S;
                    if (item.ContainsKey("voice"))
                        newPost.voice = item["voice"].S;
                    if (item.ContainsKey("status"))
                        newPost.status = item["status"].S;
                    if (item.ContainsKey("url"))
                        newPost.url = item["url"].S;
                    posts.Add(newPost);
                }
            }
            return posts;
        }
    }

    public class Request
    {
        public string postId { get; set; }
    }
}
