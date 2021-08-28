#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "@aws-cdk/core";
import { DiligenceDoerStack } from "../lib/diligence-doer-stack";
import * as dotenv from "dotenv";
dotenv.config();

const app = new cdk.App();
new DiligenceDoerStack(app, "DiligenceDoerStack", {
  env: { region: "us-east-1" },
});
