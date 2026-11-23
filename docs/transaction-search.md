# Enabling X-Ray Transaction Search

When telemetry is enabled (`TITILER_CMR_TELEMETRY_ENABLED=TRUE`), titiler-cmr exports OpenTelemetry spans to X-Ray's
OTLP endpoint (`https://xray.{region}.amazonaws.com/v1/traces`). See
[Observability](architecture.md#observability-otel-x-ray) for how tracing is wired up.

That endpoint only accepts spans when **CloudWatch Transaction Search** is enabled for the account and region. Without
it, no traces appear and the function logs `OTLP export failed: status=... body=...` on each export.

Enabling Transaction Search changes how traces are handled in a few ways:

- **Account-wide**: affects every service in the account and region that sends traces.
- **Backwards compatible**: segments sent with `PutTraceSegments` (X-Ray SDK, daemon, or Lambda `Tracing.ACTIVE`) are
  still ingested, converted to spans.
- **Storage**: spans go to the `aws/spans` log group, shared by every service in the account and region.
- **Pricing**: billed as span ingestion and indexing instead of per trace.

## Enabling it

We suggest that you don't manage this from an application stack. The CloudFormation `AWS::XRay::TransactionSearchConfig`
is a singleton per account and region, so deleting the stack that owns it changes tracing for every service. We
recommend using the CLI or console, or an account-baseline stack.

### 1. Allow X-Ray to write to CloudWatch Logs

First, define the permissions by Policy from the
[AWS docs](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/Enable-TransactionSearch.html#CloudWatch-Transaction-Search-EnableAPI),
which are the source of truth:

```bash
REGION=<your AWS region>
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "TransactionSearchXRayAccess",
    "Effect": "Allow",
    "Principal": {"Service": "xray.amazonaws.com"},
    "Action": "logs:PutLogEvents",
    "Resource": [
      "arn:aws:logs:${REGION}:${ACCOUNT}:log-group:aws/spans:*",
      "arn:aws:logs:${REGION}:${ACCOUNT}:log-group:/aws/application-signals/data:*"
    ],
    "Condition": {
      "ArnLike": {"aws:SourceArn": "arn:aws:xray:${REGION}:${ACCOUNT}:*"},
      "StringEquals": {"aws:SourceAccount": "${ACCOUNT}"}
    }
  }]
}
EOF
)
```

### 2. Switch the trace segment destination

```bash
dest=$(aws xray get-trace-segment-destination --region "$REGION" \
  --query Destination --output text)
if [ "$dest" != "CloudWatchLogs" ]; then
  aws logs put-resource-policy --region "$REGION" \
    --policy-name TransactionSearchAccess --policy-document "$POLICY"
  aws xray update-trace-segment-destination --region "$REGION" \
    --destination CloudWatchLogs
fi
```

The change is asynchronous. Wait until `aws xray get-trace-segment-destination --region "$REGION"` reports
`Status: ACTIVE`.

### 3. Set retention on `aws/spans`

The log group is created when the first spans arrive. Then:

```bash
aws logs put-retention-policy --region "$REGION" \
  --log-group-name aws/spans --retention-in-days 30
```

### 4. (Optional) Adjust the indexing percentage

The default 1% is free. Raise it only if search results are too sparse. Check the current rules with
`aws xray get-indexing-rules --region "$REGION"`.

## Verifying

1. Send a few requests to titiler-cmr.
2. Check that `OTLP export failed` errors have stopped in the function's log group.
3. Look up the `xray_trace_id` from a titiler-cmr log record in CloudWatch (Application Signals > Transaction Search).

## Rolling back

```bash
aws xray update-trace-segment-destination --region "$REGION" --destination XRay
```

titiler-cmr traces stop appearing after rollback, since the OTLP endpoint requires Transaction Search.

## References

- [CloudWatch Transaction Search](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Transaction-Search.html)
- [Getting started with Transaction Search](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Transaction-Search-getting-started.html)
- [Amazon CloudWatch pricing](https://aws.amazon.com/cloudwatch/pricing/)
- [Migrating from X-Ray instrumentation to OpenTelemetry](https://docs.aws.amazon.com/xray/latest/devguide/xray-sdk-migration.html)
