using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace NewPosts
{
    public class Function
    {
        private readonly IAmazonDynamoDB _dbClient;
        private readonly IAmazonSimpleNotificationService _snsClient;

        /// <summary>
        /// Default constructor that Lambda will invoke.
        /// </summary>
        public Function()
        {
            _dbClient = new AmazonDynamoDBClient();
            _snsClient = new AmazonSimpleNotificationServiceClient();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:ApiGateway.Functions"/> class.
        /// For testing purposes.
        /// </summary>
        /// <param name="dbClient">Db client.</param>
        /// <param name="snsClient">Sns client.</param>
        public Function(IAmazonDynamoDB dbClient, IAmazonSimpleNotificationService snsClient)
        {
            _dbClient = dbClient ?? throw new ArgumentNullException(nameof(dbClient));
            _snsClient = snsClient ?? throw new ArgumentNullException(nameof(snsClient));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<string> FunctionHandler(Post post, ILambdaContext context)
        {
            // Declare the logger.
            var logger = context.Logger;
            post.id = Guid.NewGuid().ToString();
            post.status = "PROCESSING";
            logger.LogLine($"Generating new DynamoDB record, with ID: {post.id}");
            logger.LogLine($"Input Text: {post.text}");
            logger.LogLine($"Selected voice: {post.voice}");

            // Creating new record in DynamoDB table
            var tableName = Environment.GetEnvironmentVariable("DB_TABLE_NAME");
            await _dbClient.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>
                    {
                        {"id", new AttributeValue(post.id)},
                        {"text", new AttributeValue(post.text)},
                        {"voice", new AttributeValue(post.voice)},
                        {"status", new AttributeValue(post.status)}
                    }
            });

            //Sending notification about new post to SNS
            var topicarn = Environment.GetEnvironmentVariable("SNS_TOPIC");
            await _snsClient.PublishAsync(new PublishRequest
            {
                TopicArn = topicarn,
                Message = post.id
            });

            //Return response
            return post.id;
        }
    }
}
