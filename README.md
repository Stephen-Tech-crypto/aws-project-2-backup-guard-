# BackupGuard: Backup Success/Failure Alert

A serverless AWS product that watches nightly **Amazon EBS snapshots** and makes failures impossible to miss.
A virtual OLED shows the last good backup time, and a browser buzzer sounds when a backup fails.
A NOC wall view shows one tile per volume in green, amber or red.

Built for the AWS Learner Lab (region `us-east-1`, role `LabRole`).

## Live demo

- Virtual OLED: https://jcjgqz4mxcbxvbezflxnsmtzhy0owbit.lambda-url.us-east-1.on.aws/
- NOC wall: https://jcjgqz4mxcbxvbezflxnsmtzhy0owbit.lambda-url.us-east-1.on.aws/wall

These links work only while the Learner Lab resources exist (they stop after `teardown`).

## How it works

1. EventBridge runs `bg-make-snapshot` every night (20:00 UTC) and snapshots every volume tagged `Backup=nightly`.
2. Amazon EBS publishes the real result (succeeded or failed) of each snapshot.
3. An EventBridge rule sends that event to `bg-record-result`, which updates DynamoDB, e-mails through SNS and,
   on failure, retries automatically (up to 2 times).
4. `bg-watchdog` runs every 5 minutes: escalation after 30 minutes unacknowledged, "overdue" after 26 hours
   with no backup, and a display-offline alert if the wall stops checking in.
5. `bg-device` (Lambda Function URL) serves the OLED page, the wall page, `/status` and `/ack`.

| State | Meaning | Sound |
|---|---|---|
| GREEN | Last backup succeeded and is under 24 h old | none |
| AMBER | Retry in progress, backup late, or waiting for first backup | none |
| RED | Retries failed, or no backup in 26 h | buzzer until acknowledged |

## AWS services

Amazon EBS, Amazon EventBridge, AWS Lambda (Function URL), Amazon DynamoDB, Amazon SNS,
IAM (LabRole), Amazon CloudWatch Logs, AWS CloudShell.

## Deploy (in AWS CloudShell, us-east-1)

```bash
python3 deploy.py deploy --email you@example.com   # then confirm the SNS e-mail
python3 deploy.py test-ok                          # all volumes green
python3 deploy.py snapshot-now                     # real snapshots
python3 deploy.py test-fail --vol db-01            # simulated failure: amber + auto-retry
python3 deploy.py test-red  --vol db-01            # forced red + escalation
python3 deploy.py test-ok   --vol db-01            # recovery
python3 deploy.py teardown                         # delete everything
```

Open the printed Function URL, then click **Enable sound** once (browsers block audio until you click).

## Testing notes

Failure events are simulated with the real EBS event structure because real snapshot failures cannot be
forced on demand. The retry snapshots, success events, e-mails, DynamoDB updates and logs are real.

## Limitations

The demo URL is public with no login; only EBS snapshots are monitored; the nightly schedule was configured
but not yet observed firing during testing.

## Repository contents

- `deploy.py`: one-file deployer (creates, tests and deletes all resources; contains the Lambda source and both pages)
- `README.md`: this file
