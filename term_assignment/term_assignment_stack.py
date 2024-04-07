from aws_cdk import (
    aws_s3 as s3,
    aws_kinesis as kinesis,
    aws_ec2 as ec2,
    aws_iam as iam,
    Stack,
    RemovalPolicy,
    CfnOutput,
    aws_dynamodb as dynamodb,
    aws_sns_subscriptions as subscriptions,
    aws_sns as sns,
    aws_lambda as lambda_,
    Duration,
    aws_glue as glue,
    aws_kinesisfirehose as firehose,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_apigateway as apigateway
    
)

from constructs import Construct

class TermAssignmentStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create an S3 bucket
        self.data_bucket = s3.Bucket(
            self, "DataBucket-ecommerce-stream",
            removal_policy=RemovalPolicy.DESTROY,  # Automatically delete bucket on stack destruction (for testing purposes)
        )

        # Output the bucket name
        CfnOutput(
            self, "DataBucket-ecommerce-stream-Name",
            value=self.data_bucket.bucket_name,
            description="The name of the S3 bucket for storing data and scripts."
        )

        # Create a Kinesis data stream
        self.kinesis_stream = kinesis.Stream(
            self, "EcommerceDataStream",
            stream_name="ecommerce-raw-user-activity-stream"
        )

        # Output the stream name
        CfnOutput(
            self, "KinesisDataStreamName",
            value=self.kinesis_stream.stream_name,
            description="The name of the Kinesis data stream."
        )
        ami = ec2.AmazonLinuxImage(generation=ec2.AmazonLinuxGeneration.AMAZON_LINUX_2)
        # Create a new VPC or use an existing one
        vpc = ec2.Vpc(self, "Vpc", max_azs=2)  # Creating a new VPC with 2 Availability Zones

        # IAM role for EC2 to access S3 and Kinesis
        role = iam.Role(
            self, "InstanceRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonKinesisFullAccess")
            ]
        )

        # Security group for SSH access
        sg = ec2.SecurityGroup(
            self, "SecurityGroup",
            vpc=vpc,
            description="Allow SSH access",
            allow_all_outbound=True
        )
        sg.add_ingress_rule(ec2.Peer.any_ipv4(), ec2.Port.tcp(22), "Allow SSH access")

        # Create an EC2 instance
        instance = ec2.Instance(
            self, "Instance",
            instance_type=ec2.InstanceType("t2.micro"),
            machine_image=ami,
            vpc=vpc,
            role=role,
            security_group=sg,
            key_name="momo",
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC)
        )

        # Bootstrap script for EC2
        user_data_script = f"""#!/bin/bash
exec > /home/ec2-user/user-data.log 2>&1
echo "Starting user data script execution..."

yum update -y
yum install -y python3
pip3 install boto3 || echo 'boto3 is already installed.'

while true; do
    if aws s3 ls "s3://{self.data_bucket.bucket_name}/stream-data-app-simulation.py"; then
        break
    fi
    sleep 10
done

aws s3 cp "s3://{self.data_bucket.bucket_name}/stream-data-app-simulation.py" /home/ec2-user/stream-data-app-simulation.py

chmod +x /home/ec2-user/stream-data-app-simulation.py

export S3_BUCKET="{self.data_bucket.bucket_name}"
export S3_KEY="2019-Nov-sample.csv"
export KINESIS_STREAM_NAME="{self.kinesis_stream.stream_name}"

echo "Running the Python script..."
python3 /home/ec2-user/stream-data-app-simulation.py > /home/ec2-user/script.log 2>&1
"""
        instance.add_user_data(user_data_script)

        self.user_activity_table = dynamodb.Table(
            self,
            "UserActivityTable",
            partition_key=dynamodb.Attribute(
                name="user_id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="txn_timestamp",
                type=dynamodb.AttributeType.STRING
            ),
            removal_policy=RemovalPolicy.DESTROY  # Automatically delete the table when the stack is destroyed (use wisely)
        )

        # Output the DynamoDB table name
        CfnOutput(
            self,
            "UserActivityTableName",
            value=self.user_activity_table.table_name,
            description="The name of the DynamoDB table for storing user activities."
        )

        # Create SNS topic for DDoS alerts
        self.alert_topic = sns.Topic(
            self,
            "ClickstreamAnalyticsTopic",
            display_name="Clickstream Analytics"
        )

        # Add an email subscription to the topic
        self.alert_topic.add_subscription(
            subscriptions.EmailSubscription("mrox165321@gmail.com")
        )

        # Output the SNS topic ARN
        CfnOutput(
            self,
            "AlertTopicARN",
            value=self.alert_topic.topic_arn,
            description="The ARN of the SNS topic for DDoS alerts."
        )

        lambda_function = lambda_.Function(
            self,
            "DataStreamProcessor",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="processor.lambda_handler",
            code=lambda_.Code.from_asset("term_assignment/lambda"),
            environment={
                'TABLE_NAME': self.user_activity_table.table_name,
                'SNS_TOPIC_ARN': self.alert_topic.topic_arn
            },
            timeout=Duration.seconds(300), 
            memory_size=256  
        )

        # Grant permissions to Lambda function to access DynamoDB and SNS
        self.user_activity_table.grant_read_write_data(lambda_function)
        self.alert_topic.grant_publish(lambda_function)

     
        glue_database = glue.CfnDatabase(
        self,
        "MyGlueDatabase",
        catalog_id=self.account,
        database_input=glue.CfnDatabase.DatabaseInputProperty(
            name="my_glue_database"
        )
    )

        # Define the Glue table using CfnTable
        glue_table = glue.CfnTable(
            self,
            "MyGlueTable",
            catalog_id=self.account,
            database_name=glue_database.ref,
            table_input=glue.CfnTable.TableInputProperty(
                name="my_glue_table",
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        glue.CfnTable.ColumnProperty(name="event_time", type="string"),
                        glue.CfnTable.ColumnProperty(name="event_type", type="string"),
                        glue.CfnTable.ColumnProperty(name="product_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="category_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="category_code", type="string"),
                        glue.CfnTable.ColumnProperty(name="brand", type="string"),
                        glue.CfnTable.ColumnProperty(name="price", type="string"),  # Assuming as string; change if needed
                        glue.CfnTable.ColumnProperty(name="user_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="user_session", type="string"),
                        glue.CfnTable.ColumnProperty(name="txn_timestamp", type="string")
                    ],
                    location=f"s3://{self.data_bucket.bucket_name}/processed-data/",  # Specify the S3 location if needed
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    )
                ),
                table_type="EXTERNAL_TABLE"  # Specify based on your setup
            )
        )

        # IAM role for Kinesis Firehose
        firehose_role = iam.Role(
            self,
            "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSLambda_FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonKinesisFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonKinesisFirehoseFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSGlueConsoleFullAccess"),
                #iam.ManagedPolicy.from_aws_managed_policy_name("AWSGlueServiceRole")

            ]
        )

        # Create Firehose delivery stream
        firehose_stream = firehose.CfnDeliveryStream(
            self,
            "MyDeliveryStream",
            delivery_stream_type="KinesisStreamAsSource",
            kinesis_stream_source_configuration=firehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
                kinesis_stream_arn=self.kinesis_stream.stream_arn,
                role_arn=firehose_role.role_arn
            ),
            extended_s3_destination_configuration=firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=self.data_bucket.bucket_arn,
                role_arn=firehose_role.role_arn,
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=30,  # Buffer time
                    size_in_m_bs=64  # Buffer size, smaller due to Lambda payload limit and potential expansion
                ),
                compression_format="UNCOMPRESSED",
                prefix='processed-data/',  # Data will be stored in the 'processed-data/' folder in the bucket
                error_output_prefix='error-data/',
                data_format_conversion_configuration=firehose.CfnDeliveryStream.DataFormatConversionConfigurationProperty(
                    schema_configuration=firehose.CfnDeliveryStream.SchemaConfigurationProperty(
                        role_arn=firehose_role.role_arn,
                        #database_name=glue_database.database_name,
                        database_name=glue_database.ref,
                        table_name="my_glue_table",
                        region="us-east-1"  # Replace with your AWS region
                    ),
                    input_format_configuration=firehose.CfnDeliveryStream.InputFormatConfigurationProperty(
                        deserializer=firehose.CfnDeliveryStream.DeserializerProperty(
                            hive_json_ser_de=firehose.CfnDeliveryStream.HiveJsonSerDeProperty()
                           # open_x_json_ser_de={}
                        )
                    ),
                    output_format_configuration=firehose.CfnDeliveryStream.OutputFormatConfigurationProperty(
                        serializer=firehose.CfnDeliveryStream.SerializerProperty(
                            parquet_ser_de=firehose.CfnDeliveryStream.ParquetSerDeProperty()
                        )
                    ),
                    enabled=True
                ),
                processing_configuration=firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
                    enabled=True,
                    processors=[firehose.CfnDeliveryStream.ProcessorProperty(
                        type="Lambda",
                        parameters=[firehose.CfnDeliveryStream.ProcessorParameterProperty(
                            parameter_name="LambdaArn",
                            parameter_value=lambda_function.function_arn
                        )]
                    )]
                )
            )
        )

        # Update Lambda function to add permission to be invoked by Firehose
        lambda_function.add_permission(
        "FirehoseInvocation",
        action="lambda:InvokeFunction",
        principal=iam.ServicePrincipal("firehose.amazonaws.com"),
        source_arn=firehose_stream.attr_arn
    )
        
        lambda_apple = lambda_.Function(
        self, 'LambdaApple',
        runtime=lambda_.Runtime.PYTHON_3_8,
        handler='lambda_apple.handler',
        code=lambda_.Code.from_asset('term_assignment/lambda_apple'),  
        environment={
            'TABLE_NAME': self.user_activity_table.table_name,
            'S3_BUCKET': self.data_bucket.bucket_name,
            'SNS_TOPIC_ARN': self.alert_topic.topic_arn
        },
        timeout=Duration.seconds(300), 
        memory_size=256
    )
        apple_policy = iam.PolicyStatement(
        actions=[
            "dynamodb:Scan",
            "dynamodb:GetItem",
            "dynamodb:Query",
            "s3:PutObject",
            "s3:GetObject",
            "s3:ListBucket"
        ],
        resources=[
            self.user_activity_table.table_arn,
            self.data_bucket.bucket_arn,
            f"{self.data_bucket.bucket_arn}/*"
        ]
    )

        lambda_apple.add_to_role_policy(apple_policy)
        self.alert_topic.grant_publish(lambda_apple)

        lambda_samsung = lambda_.Function(
            self, 'LambdaSamsung',
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler='lambda_samsung.handler',
            code=lambda_.Code.from_asset('term_assignment/lambda_samsung'),  
            environment={
                'TABLE_NAME': self.user_activity_table.table_name,
                'S3_BUCKET': self.data_bucket.bucket_name,
                'SNS_TOPIC_ARN': self.alert_topic.topic_arn
            },
            timeout=Duration.seconds(300), 
            memory_size=256 
        )

        samsung_policy = iam.PolicyStatement(
        actions=[
            "dynamodb:Scan",
            "dynamodb:GetItem",
            "dynamodb:Query",
            "s3:PutObject",
            "s3:GetObject",
            "s3:ListBucket"
        ],
        resources=[
            self.user_activity_table.table_arn,
            self.data_bucket.bucket_arn,
            f"{self.data_bucket.bucket_arn}/*"
        ]
    )

        lambda_samsung.add_to_role_policy(samsung_policy)
        self.alert_topic.grant_publish(lambda_samsung)

        choice_state = sfn.Choice(self, "BrandChoice")

        # Define the tasks for each brand processing
        apple_task = tasks.LambdaInvoke(
            self,
            "AppleTask",
            lambda_function=lambda_apple,
            result_path="$.result"
        )

        samsung_task = tasks.LambdaInvoke(
            self,
            "SamsungTask",
            lambda_function=lambda_samsung,
            result_path="$.result"
        )

        # Configure the routing based on the brand
        choice_state.when(sfn.Condition.string_equals("$.brand", "apple"), apple_task)
        choice_state.when(sfn.Condition.string_equals("$.brand", "samsung"), samsung_task)

        # Define the state machine
        state_machine = sfn.StateMachine(
            self,
            "BrandStateMachine",
            definition=choice_state.afterwards(),
            timeout=Duration.minutes(5)
        )

        # Create an IAM role for API Gateway
        api_gateway_role = iam.Role(
            self,
            "APIGatewayStepFunctionsRole",
            assumed_by=iam.ServicePrincipal("apigateway.amazonaws.com")
        )

        # Attach a policy to the role to allow starting Step Functions executions
        api_gateway_role.add_to_policy(
            iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[state_machine.state_machine_arn]
            )
        )

        # Create an API Gateway REST API
        rest_api = apigateway.RestApi(
            self,
            "StepFunctionsTriggerAPI",
            rest_api_name="StepFunctionsTriggerAPI",
            description="API Gateway to trigger Step Functions."
        )

        # Create a resource for triggering the state machine
        trigger_resource = rest_api.root.add_resource("trigger")

        # Add a POST method to the resource
        trigger_resource.add_method(
            "POST",
            integration=apigateway.AwsIntegration(
                service="states",
                action="StartExecution",
                integration_http_method="POST",
                options=apigateway.IntegrationOptions(
                    credentials_role=api_gateway_role,
                    integration_responses=[{
                        "statusCode": "200"
                    }],
                    request_templates={
                        "application/json": f"""
                        {{
                            "input": "$util.escapeJavaScript($input.json('$'))",
                            "stateMachineArn": "{state_machine.state_machine_arn}"
                        }}
                        """
                    }
                )
            ),
            method_responses=[{
                "statusCode": "200",
                "responseModels": {
                    "application/json": apigateway.Model.EMPTY_MODEL
                }
            }]
        )

        # Deploy the API
        deployment = apigateway.Deployment(
            self,
            "Deployment",
            api=rest_api
        )
        stage_name = "prod1"

        stage = apigateway.Stage(
            self,
            "Stage",
            deployment=deployment,
            stage_name=stage_name
        )

        rest_api.deployment_stage = stage

        # Output the URL of the API
        CfnOutput(self, "APIEndpoint", value=f"{rest_api.url_for_path(trigger_resource.path)}")



