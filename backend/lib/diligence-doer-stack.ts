import * as cdk from "@aws-cdk/core";
import * as apigateway from "@aws-cdk/aws-apigateway";
import * as dynamodb from "@aws-cdk/aws-dynamodb";
import * as events from "@aws-cdk/aws-events";
import * as eventTargets from "@aws-cdk/aws-events-targets";
import * as lambda from "@aws-cdk/aws-lambda";
import { NodejsFunction } from "@aws-cdk/aws-lambda-nodejs";
import { PythonFunction } from "@aws-cdk/aws-lambda-python";

export class DiligenceDoerStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const datastore = new dynamodb.Table(this, "datastore", {
      tableName: "diligence-doer",
      partitionKey: { name: "pk", type: dynamodb.AttributeType.STRING },
      // sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
    });

    const tableauFn = new PythonFunction(this, "tableau-fn", {
      functionName: "diligence-doer-tableau",
      entry: "lambda/tableau",
      // index: "", // will default to index.py but you can override that here
      // handler: "", // will default to handler but you can override that here
      runtime: lambda.Runtime.PYTHON_3_8,
      timeout: cdk.Duration.seconds(60), // can update this to anything under 15 minutes
      environment: {
        // key/value pairs to inject as env vars into lambda fn
      },
    });

    datastore.grantReadWriteData(tableauFn);

    const githubFn = new PythonFunction(this, "github-fn", {
      functionName: "diligence-doer-github",
      entry: "lambda/github",
      // index: "", // will default to index.py but you can override that here
      // handler: "", // will default to handler but you can override that here
      runtime: lambda.Runtime.PYTHON_3_8,
      timeout: cdk.Duration.seconds(60), // can update this to anything under 15 minutes
      environment: {
        // key/value pairs to inject as env vars into lambda fn
      },
    });

    datastore.grantReadWriteData(githubFn);

    const twentyFourSevenCron = "cron(59 23 * * ? *)";

    const cronJob = new events.Rule(this, "cron-job", {
      schedule: events.Schedule.expression(twentyFourSevenCron),
    });

    cronJob.addTarget(new eventTargets.LambdaFunction(tableauFn));
    cronJob.addTarget(new eventTargets.LambdaFunction(githubFn));

    const atlassianForgeFn = new NodejsFunction(this, "atlassian-forge-fn", {
      functionName: "diligence-doer-atlassian-forge",
      entry: "lambda/atlassian-forge/index.ts",
    });

    datastore.grantReadWriteData(atlassianForgeFn);

    const api = new apigateway.RestApi(this, "diligence-doer-api", {
      restApiName: "diligence-doer-api",
      deployOptions: {
        stageName: "prod",
      },
    });

    const atlassian = api.root.addResource("atlassian");
    const issues = atlassian.addResource("issues");
    const issue = issues.addResource("{issue_id}");

    issues.addMethod(
      "POST",
      new apigateway.LambdaIntegration(atlassianForgeFn),
      {
        apiKeyRequired: true,
      }
    );

    issue.addMethod("GET", new apigateway.LambdaIntegration(atlassianForgeFn), {
      apiKeyRequired: true,
    });

    const apiKey = api.addApiKey("api-key", {
      apiKeyName: "diligence-doer-api-key",
    });

    const usagePlan = api.addUsagePlan("api-key-usage-plan", {
      name: "diligence-doer-api-usage-plan",
      throttle: {
        rateLimit: 1,
      },
    });

    usagePlan.addApiKey(apiKey);
  }
}
