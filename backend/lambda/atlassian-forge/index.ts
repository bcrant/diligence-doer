import { APIGatewayProxyEvent } from "aws-lambda";
import { DynamoDB } from "aws-sdk";
import { DocumentClient } from "aws-sdk/lib/dynamodb/document_client";

const docClient = new DynamoDB.DocumentClient();

export const getReferences = async (field: string): Promise<any> => {
  const params: DocumentClient.GetItemInput = {
    TableName: "diligence-doer",
    Key: {
      pk: field,
    },
  };

  const { Item } = await docClient.get(params).promise();

  if (!Item) {
    return { field };
  }

  return {
    field,
    ...Item
  };
};

exports.handler = async (event: APIGatewayProxyEvent) => {
  try {
    const queryString = event.queryStringParameters?.fields;

    if (!queryString) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          message: "Missing query string 'field'"
        })
      };
    }
  
    const fields = decodeURIComponent(queryString).split(",");

    const references = await Promise.all(fields.map(async(field) => await getReferences(field)));
  
    return {
      statusCode: 200,
      body: JSON.stringify({
        references
      })
    };
  } catch (err) {
    console.log(err);
    return {
      statusCode: 500,
      body: JSON.stringify({
        message: "Failed to process request",
        error: err.message,
      })
    };
  }
};
