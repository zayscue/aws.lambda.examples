using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.DynamoDBv2;
using Amazon.SimpleNotificationService;
using Amazon.DynamoDBv2.Model;
using Amazon.SimpleNotificationService.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace ApiGateway
{
    public class Functions
    {
        private readonly IAmazonDynamoDB _dbClient;
		private readonly IAmazonSimpleNotificationService _snsClient;

        /// <summary>
        /// Default constructor that Lambda will invoke.
        /// </summary>
        public Functions()
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
		public Functions(IAmazonDynamoDB dbClient, IAmazonSimpleNotificationService snsClient)
		{
			_dbClient = dbClient ?? throw new ArgumentNullException(nameof(dbClient));
			_snsClient = snsClient ?? throw new ArgumentNullException(nameof(snsClient));
		}


		/// <summary>
		/// A Lambda function to respond to HTTP Get methods from API Gateway
		/// </summary>
		/// <param name="request"></param>
		/// <returns>The list of blogs</returns>
		public APIGatewayProxyResponse Handler(APIGatewayProxyRequest request, ILambdaContext context)
		{
			// Declare the logger.
			var logger = context.Logger;

			//TODO: Get text and voice data from the api request.

			// Define item model.
			var item = new
			{
				id = Guid.NewGuid().ToString(),
				text = "",
				voice = "",
				status = "PROCESSING"
			};
			logger.LogLine($"A new post was created recieved with id {item.id}.");

			// Creating new record in DynamoDB table
			var tableName = Environment.GetEnvironmentVariable("DB_TABLE_NAME");
			_dbClient.PutItemAsync(new PutItemRequest
			{
				TableName = tableName,
				Item = new Dictionary<string, AttributeValue> 
				{
					{"id", new AttributeValue(item.id)},
					{"text", new AttributeValue(item.text)},
					{"voice", new AttributeValue(item.voice)},
					{"status", new AttributeValue(item.status)}
				}
			}).GetAwaiter().GetResult();
			logger.LogLine($"A new post was insert into the dynamodb table {tableName} with id {item.id}.");

			//Sending notification about new post to SNS
			var topicName = Environment.GetEnvironmentVariable("SNS_TOPIC");
			_snsClient.PublishAsync(new PublishRequest
			{
				TopicArn = topicName,
				Message = item.id
			}).GetAwaiter().GetResult();
			logger.LogLine($"A new post was created message was published on the {topicName} sns topic for post {item.id}.");

			//Return response
			var response = new APIGatewayProxyResponse
			{
				StatusCode = (int)HttpStatusCode.OK,
				Body = $"{item.id}",
				Headers = new Dictionary<string, string> { { "Content-Type", "text/plain" } }
			};
            return response;
        }
    }
}
