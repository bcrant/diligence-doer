import * as cdk from "@aws-cdk/core";
import * as dynamodb from "@aws-cdk/aws-dynamodb";
import * as apigateway from "@aws-cdk/aws-apigateway";
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
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
    });

    const tableauFn = new PythonFunction(this, "tableau-fn", {
      functionName: "diligence-doer-tableau",
      entry: "./lambda/tableau",
      // index: "", // will default to index.py but you can override that here
      // handler: "", // will default to handler but you can override that here
      runtime: lambda.Runtime.PYTHON_3_8,
      timeout: cdk.Duration.seconds(60), // can update this to anything under 15 minutes
      environment: {
        TABLEAU_SERVER_URL: process.env.TABLEAU_SERVER_URL as string,
        TABLEAU_PAT_NAME: process.env.TABLEAU_PAT_NAME as string,
        TABLEAU_PAT: process.env.TABLEAU_PAT as string,
      },
    });

    datastore.grantReadWriteData(tableauFn);

    const githubFn = new PythonFunction(this, "github-fn", {
      functionName: "diligence-doer-github",
      entry: "./lambda/github",
      // index: "", // will default to index.py but you can override that here
      // handler: "", // will default to handler but you can override that here
      runtime: lambda.Runtime.PYTHON_3_8,
      timeout: cdk.Duration.seconds(60), // can update this to anything under 15 minutes
      environment: {
        // key/value pairs to inject as env vars into lambda fn
        GITHUB_PAT: process.env.GITHUB_PAT as string,
        GITHUB_REPO_OWNER: process.env.GITHUB_REPO_OWNER as string,
        GITHUB_REPO_NAME: process.env.GITHUB_REPO_NAME as string,
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
      entry: "./lambda/atlassian-forge/index.ts",
    });

    datastore.grantReadWriteData(atlassianForgeFn);

    const api = new apigateway.RestApi(this, "diligence-doer-api", {
      restApiName: "diligence-doer-api",
      deployOptions: {
        stageName: "prod",
      },
    });

    const forge = api.root.addResource("forge");

    forge.addMethod("GET", new apigateway.LambdaIntegration(atlassianForgeFn), {
      apiKeyRequired: true,
    });

    const apiKey = api.addApiKey("api-key", {
      apiKeyName: "diligence-doer-api-key",
    });

    const usagePlan = api.addUsagePlan("api-key-usage-plan", {
      name: "diligence-doer-api-usage-plan",
    });

    usagePlan.addApiKey(apiKey);
    usagePlan.addApiStage({ stage: api.deploymentStage });
  }
}
