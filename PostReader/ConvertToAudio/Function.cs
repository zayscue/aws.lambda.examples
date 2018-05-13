using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.SNSEvents;
using Amazon.Polly;
using Amazon.Polly.Model;
using Amazon.S3;
using Amazon.S3.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace ConvertToAudio
{
    public class Function
    {
        private readonly IAmazonDynamoDB _dbClient;
        private readonly IAmazonS3 _s3Client;
        private readonly IAmazonPolly _pollyClient;

        /// <summary>
        /// Default constructor that Lambda will invoke.
        /// </summary>
        public Function()
        {
            _dbClient = new AmazonDynamoDBClient();
            _s3Client = new AmazonS3Client();
            _pollyClient = new AmazonPollyClient();
        }

        /// <summary>
        /// A constructor with all the dependencies passed in 
        /// for testing.
        /// </summary>
        /// <param name="dbClient"></param>
        public Function(IAmazonDynamoDB dbClient, IAmazonS3 s3Client, IAmazonPolly pollyClient)
        {
            _dbClient = dbClient ?? throw new ArgumentNullException(nameof(dbClient));
            _s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
            _pollyClient = pollyClient ?? throw new ArgumentNullException(nameof(pollyClient));
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(SNSEvent @event, ILambdaContext context)
        {
            var logger = context.Logger;
            var postId = @event.Records[0].Sns.Message;

            logger.LogLine($"Text to Speech function.  Post ID in DynamoDB: {postId}");

            // Retrieving information about the post from DynamoDB table
            var tableName = Environment.GetEnvironmentVariable("DB_TABLE_NAME");
            var queryResult = await _dbClient.QueryAsync(new QueryRequest
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
                                new AttributeValue(postId)
                            }
                        }
                    }
                }
            });
            var post = queryResult.Items[0];
            var text = post["text"].S;
            var voice = post["voice"].S;

            // Chunk and Synthesize text into speech.
            var chunkSize = 1000;
			var textBlocks = text.Chunk(chunkSize).ToArray();
            var path = System.IO.Path.Combine("/tmp/", postId);
            for(var i = 0; i < textBlocks.Length; i++)
            {
                var textBlock = textBlocks[i];
                var pollyResponse = await _pollyClient.SynthesizeSpeechAsync(new SynthesizeSpeechRequest
                {
                    OutputFormat = "mp3",
                    Text = textBlock,
                    VoiceId = voice
                });
                if(pollyResponse?.AudioStream != null)
                {
                    using (var fileStream = System.IO.File.Open(path, System.IO.FileMode.Append))
                    {
                        await pollyResponse.AudioStream.CopyToAsync(fileStream);
                    }
                    logger.LogLine($"Finished synthesizing text chunck {i + 1}");
                }
            }

            //Put synthesized audio into s3 bucket
            var bucketName = Environment.GetEnvironmentVariable("BUCKET_NAME");
            await _s3Client.UploadObjectFromFilePathAsync(bucketName, $"{postId}.mp3", path, null);
            await _s3Client.PutACLAsync(new PutACLRequest
            {
                BucketName = bucketName,
                Key = $"{postId}.mp3",
                CannedACL = S3CannedACL.PublicRead
            });
            var bucketLocation = await _s3Client.GetBucketLocationAsync(new GetBucketLocationRequest
            {
                BucketName = bucketName
            });
            var region = bucketLocation.Location;

            // Update DynamoDB with the audio file url and the "UPDATED" status
            var urlBeginning = region == null || string.IsNullOrWhiteSpace(region.Value)
                ? "https://s3.amazonaws.com/" 
                : $"https://s3-{region.Value}.amazonaws.com/";
            var url = string.Concat(urlBeginning, bucketName, "/", postId, ".mp3");
            await _dbClient.UpdateItemAsync(new UpdateItemRequest
            {
                TableName = tableName,
                Key = new Dictionary<string, AttributeValue>
                {
                    { "id", new AttributeValue(postId) }
                },
                UpdateExpression = "SET #statusAtt = :statusValue, #urlAtt = :urlValue",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":statusValue", new AttributeValue("UPDATED") },
                    { ":urlValue", new AttributeValue(url) }
                },
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    { "#statusAtt", "status" },
                    { "#urlAtt", "url" }
                }
            });
            logger.LogLine($"Finished updating post ,ID: {postId}, in table {tableName}");
        }
    }
}
