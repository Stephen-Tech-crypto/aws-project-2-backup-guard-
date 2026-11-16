#!/usr/bin/env python3
"""
BackupGuard v2 - "a smoke detector for backups"  (AWS Learner Lab, run in CloudShell)

  python3 deploy.py deploy --email you@example.com
  python3 deploy.py url
  python3 deploy.py snapshot-now           # real snapshots of all demo volumes
  python3 deploy.py test-fail [--vol db-01]   # failed snapshot -> AMBER (retrying)
  python3 deploy.py test-red  [--vol db-01]   # hard RED + escalated (buzzer, email in <=5 min)
  python3 deploy.py test-ok   [--vol all]     # recovery -> GREEN
  python3 deploy.py teardown

Views:   <function-url>/        virtual OLED device (overall status)
         <function-url>/wall    NOC wall display (per-volume tiles)
"""
import argparse, datetime, io, json, zipfile
import boto3
from botocore.exceptions import ClientError

REGION, DR_REGION = "us-east-1", "us-west-2"
TABLE, TOPIC = "BackupGuardStatus", "BackupGuardAlerts"
FN_SNAP, FN_REC, FN_WEB, FN_WATCH = "bg-make-snapshot", "bg-record-result", "bg-device", "bg-watchdog"
RULE_SCHED, RULE_EVT, RULE_WATCH = "bg-nightly-schedule", "bg-snapshot-events", "bg-watchdog-5min"
TAG = {"Key": "Project", "Value": "BackupGuard"}
DEMO_VOLUMES = ["web-01", "db-01", "app-01"]

# =============================================================== Lambda sources
COMMON = r'''
import datetime
def now(): return datetime.datetime.utcnow()
def iso(d): return d.isoformat() + "Z"
def parse(ts): return datetime.datetime.fromisoformat(ts.rstrip("Z"))

def state(it, n=None):
    """Return (GREEN|AMBER|RED, age_hours_or_None) for one volume item."""
    n = n or now()
    ls = it.get("last_success")
    age = (n - parse(ls)).total_seconds() / 3600 if ls else None
    if it.get("status") == "FAILED":
        fs = it.get("failed_since")
        mins = (n - parse(fs)).total_seconds() / 60 if fs else 999
        if int(it.get("retries", 0)) <= 2 and mins < 30:
            return "AMBER", age          # auto-retry still in progress
        return "RED", age
    if age is None:                      # never backed up
        cr = it.get("created")
        if cr and (n - parse(cr)).total_seconds() < 1800:
            return "AMBER", age          # grace period after deploy
        return "RED", age
    if age > 26: return "RED", age       # watchdog: silence is not safety
    if age > 24: return "AMBER", age     # running late
    return "GREEN", age
'''

MAKE_SNAPSHOT = r'''
import boto3, os
ec2 = boto3.client("ec2")
t = boto3.resource("dynamodb").Table(os.environ["TABLE"])
def handler(e, c):
    vols = ec2.describe_volumes(Filters=[{"Name": "tag:Backup", "Values": ["nightly"]}])["Volumes"]
    made = []
    for v in vols:
        name = next((x["Value"] for x in v.get("Tags", []) if x["Key"] == "Name"), v["VolumeId"])
        t.update_item(Key={"id": "vol#" + v["VolumeId"]},
            UpdateExpression="SET #n = if_not_exists(#n, :n), volume = :v",
            ExpressionAttributeNames={"#n": "name"},
            ExpressionAttributeValues={":n": name, ":v": v["VolumeId"]})
        r = ec2.create_snapshot(VolumeId=v["VolumeId"], Description="BackupGuard nightly",
            TagSpecifications=[{"ResourceType": "snapshot",
                                "Tags": [{"Key": "Project", "Value": "BackupGuard"}]}])
        made.append(r["SnapshotId"])
    return made
'''

RECORD_RESULT = r'''
import boto3, os
from common import now, iso
t = boto3.resource("dynamodb").Table(os.environ["TABLE"])
ec2 = boto3.client("ec2")
sns = boto3.client("sns")
TOPIC = os.environ["TOPIC_ARN"]
TAGS = [{"ResourceType": "snapshot", "Tags": [{"Key": "Project", "Value": "BackupGuard"}]}]
CLEAR = "REMOVE failed_since, red_since, escalated_sent, acked_by, acked_at"

def handler(e, c):
    d = e["detail"]
    ts = iso(now())
    ok = d.get("result") == "succeeded"
    vol = d.get("source", "").split("/")[-1] or "unknown"
    key = {"id": "vol#" + vol}

    # nightly history strip (a failure stays visible for that night)
    t.update_item(Key={"id": "hist#" + ts[:10]},
        UpdateExpression="SET #d = :d ADD " + ("oks" if ok else "fails") + " :one",
        ExpressionAttributeNames={"#d": "day"},
        ExpressionAttributeValues={":d": ts[:10], ":one": 1})

    cur = t.get_item(Key=key).get("Item", {})
    name = cur.get("name", vol)

    if ok:
        t.update_item(Key=key,
            UpdateExpression="SET #s=:s, last_success=:t, retries=:z, acked=:f, volume=:v " + CLEAR,
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "OK", ":t": ts, ":z": 0, ":f": False, ":v": vol})
        if cur.get("status") == "FAILED" or cur.get("red_since"):
            sns.publish(TopicArn=TOPIC, Subject="Backup RECOVERED: " + name,
                        Message="%s (%s) backed up successfully at %s" % (name, vol, ts))
        snap = d.get("snapshot_id", "").split("/")[-1]
        if snap.startswith("snap-"):          # cross-region DR copy
            try:
                boto3.client("ec2", region_name="us-west-2").copy_snapshot(
                    SourceRegion="us-east-1", SourceSnapshotId=snap,
                    Description="BackupGuard DR copy", TagSpecifications=TAGS)
            except Exception as ex:
                print("DR copy failed:", ex)
    else:
        retries = int(cur.get("retries", 0)) + 1
        since = cur.get("failed_since") if cur.get("status") == "FAILED" else ts
        t.update_item(Key=key,
            UpdateExpression="SET #s=:s, retries=:r, failed_since=:f, acked=:a, volume=:v "
                             "REMOVE acked_by, acked_at",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "FAILED", ":r": retries, ":f": since,
                                       ":a": False, ":v": vol})
        sns.publish(TopicArn=TOPIC, Subject="BACKUP FAILED: " + name,
            Message="%s (%s) snapshot FAILED at %s (failure #%d). Auto-retry %s."
                    % (name, vol, ts, retries, "started" if retries <= 2 else "exhausted"))
        if retries <= 2 and vol.startswith("vol-"):   # auto-remediation, capped
            try:
                ec2.create_snapshot(VolumeId=vol, Description="BackupGuard retry",
                                    TagSpecifications=TAGS)
            except Exception as ex:
                print("Retry failed:", ex)
'''

WATCHDOG = r'''
import boto3, os
from common import now, iso, parse, state
t = boto3.resource("dynamodb").Table(os.environ["TABLE"])
sns = boto3.client("sns")
TOPIC = os.environ["TOPIC_ARN"]

def handler(e, c):
    n = now()
    items = t.scan(FilterExpression="begins_with(id, :p)",
                   ExpressionAttributeValues={":p": "vol#"})["Items"]
    for it in items:
        st, age = state(it, n)
        key = {"id": it["id"]}
        name = it.get("name", it["id"][4:])
        if st == "RED":
            if not it.get("red_since"):
                t.update_item(Key=key, UpdateExpression="SET red_since=:r",
                              ExpressionAttributeValues={":r": iso(n)})
                if it.get("status") != "FAILED":
                    sns.publish(TopicArn=TOPIC, Subject="BACKUP OVERDUE: " + name,
                        Message="No successful backup for %s in over 26 hours." % name)
            elif (not it.get("acked") and not it.get("escalated_sent")
                  and (n - parse(it["red_since"])).total_seconds() > 1800):
                sns.publish(TopicArn=TOPIC, Subject="ESCALATION - UNACKNOWLEDGED: " + name,
                    Message="%s has been RED for over 30 minutes with nobody acknowledging." % name)
                t.update_item(Key=key, UpdateExpression="SET escalated_sent=:t",
                              ExpressionAttributeValues={":t": True})
        elif it.get("red_since"):
            t.update_item(Key=key, UpdateExpression="REMOVE red_since, escalated_sent")

    # fail-safe: is anybody still watching? (wall display heartbeat)
    hb = t.get_item(Key={"id": "hb"}).get("Item")
    if hb and not hb.get("alerted") and (n - parse(hb["ts"])).total_seconds() > 300:
        sns.publish(TopicArn=TOPIC, Subject="BackupGuard DISPLAY OFFLINE",
            Message="The BackupGuard display has not checked in for 5+ minutes. Nobody may be watching.")
        t.update_item(Key={"id": "hb"}, UpdateExpression="SET alerted=:t",
                      ExpressionAttributeValues={":t": True})
'''

DEVICE = r'''
import boto3, json, os
from common import now, iso, parse, state
t = boto3.resource("dynamodb").Table(os.environ["TABLE"])
HERE = os.path.dirname(__file__)
def page(name): return open(os.path.join(HERE, name)).read()
def scan(prefix):
    return t.scan(FilterExpression="begins_with(id, :p)",
                  ExpressionAttributeValues={":p": prefix})["Items"]

def status(hb):
    n = now()
    if hb:
        t.put_item(Item={"id": "hb", "ts": iso(n)})     # display heartbeat (clears alert)
    order = {"GREEN": 0, "AMBER": 1, "RED": 2}
    vs, overall, alarm = [], "GREEN", "none"
    for it in scan("vol#"):
        st, age = state(it, n)
        acked = bool(it.get("acked"))
        esc = (st == "RED" and not acked and bool(it.get("red_since"))
               and (n - parse(it["red_since"])).total_seconds() > 1800)
        if st == "RED" and not acked:
            if alarm == "none": alarm = "normal"
            if esc: alarm = "escalated"
        if order[st] > order[overall]: overall = st
        vs.append({"id": it.get("volume", it["id"][4:]), "name": it.get("name", it["id"][4:]),
                   "state": st, "last_success": it.get("last_success"), "age_h": age,
                   "acked": acked, "acked_by": it.get("acked_by"),
                   "acked_at": it.get("acked_at"), "escalated": esc})
    vs.sort(key=lambda v: v["name"])
    hist = sorted(scan("hist#"), key=lambda h: h["day"])[-14:]
    hist = [int(h.get("fails", 0)) == 0 and h.get("ok", True) is not False for h in hist]
    lasts = [v["last_success"] for v in vs]
    oldest = None if (not lasts or None in lasts) else min(lasts)
    ages = [v["age_h"] for v in vs if v["age_h"] is not None]
    return {"overall": overall, "alarm": alarm, "vols": vs, "hist": hist,
            "last_success": oldest, "age_h": max(ages) if ages else None, "now": iso(n)}

def handler(e, c):
    path = e.get("rawPath", "/")
    q = e.get("queryStringParameters") or {}
    if path.endswith("/ack"):
        by, vol = (q.get("by") or "operator")[:30], q.get("vol", "all")
        for it in scan("vol#"):
            if vol == "all" or it.get("volume") == vol:
                t.update_item(Key={"id": it["id"]},
                    UpdateExpression="SET acked=:a, acked_by=:b, acked_at=:t",
                    ExpressionAttributeValues={":a": True, ":b": by, ":t": iso(now())})
        return {"statusCode": 200, "body": "ok"}
    if path.endswith("/status"):
        return {"statusCode": 200,
                "headers": {"Content-Type": "application/json", "Cache-Control": "no-store"},
                "body": json.dumps(status(q.get("hb") == "1"), default=str)}
    if path.endswith("/wall"):
        return {"statusCode": 200, "headers": {"Content-Type": "text/html"}, "body": page("wall.html")}
    return {"statusCode": 200, "headers": {"Content-Type": "text/html"}, "body": page("oled.html")}
'''

OLED_HTML = r'''<!doctype html>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BackupGuard</title>
<body style="background:#111;color:#ddd;font-family:monospace;text-align:center;margin:20px">
<h2>BackupGuard virtual device</h2>
<canvas id="o" width="128" height="64"
 style="width:512px;max-width:95vw;image-rendering:pixelated;border:8px solid #333;border-radius:8px"></canvas>
<p id="msg"></p>
<button id="en">Enable buzzer</button> <button id="ack">Acknowledge</button>
<a href="wall" style="color:#8ab4ff;margin-left:12px">NOC wall view</a>
<script>
var c = document.getElementById("o"), x = c.getContext("2d");
var S = {}, audio = null, beep = null;
document.getElementById("en").onclick = function () {
  audio = new (window.AudioContext || window.webkitAudioContext)();
  document.getElementById("msg").textContent = "Buzzer armed.";
};
document.getElementById("ack").onclick = function () { fetch("ack?vol=all&by=oled").then(poll); };
function fmt(ts) {
  if (!ts) return "never";
  return new Date(ts).toLocaleString([], {dateStyle: "short", timeStyle: "short"});
}
function draw() {
  var o = S.overall || "RED";
  var col = o === "GREEN" ? "#4dff88" : (o === "AMBER" ? "#ffc247" : "#ff5050");
  var txt = o === "GREEN" ? "BACKUP OK" : (o === "AMBER" ? "BACKUP WARN" : "BACKUP FAIL");
  x.fillStyle = "#000"; x.fillRect(0, 0, 128, 64);
  x.fillStyle = col; x.font = "bold 14px monospace"; x.fillText(txt, 2, 14);
  x.font = "9px monospace";
  x.fillText("Last good:", 2, 28);
  x.fillText(fmt(S.last_success), 2, 38);
  x.fillText("At risk: " + (S.age_h == null ? "?" : S.age_h.toFixed(1) + " h"), 2, 48);
  (S.hist || []).forEach(function (ok, i) {
    x.fillStyle = ok ? "#4dff88" : "#ff5050";
    x.fillRect(2 + i * 9, 54, 7, 7);
  });
}
function buzz() {
  if (!S.alarm || S.alarm === "none") { if (beep) { clearInterval(beep); beep = null; } return; }
  if (beep) return;
  if (!audio) { document.getElementById("msg").textContent = "FAILURE - click Enable buzzer"; return; }
  beep = setInterval(function () {
    var o = audio.createOscillator();
    o.type = "square"; o.frequency.value = 2000;
    o.connect(audio.destination); o.start(); o.stop(audio.currentTime + 0.2);
  }, 600);
}
function poll() {
  fetch("status?hb=0").then(function (r) { return r.json(); })
    .then(function (j) { S = j; draw(); buzz(); });
}
poll(); setInterval(poll, 10000);
</script>
'''

WALL_HTML = r'''<!doctype html>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BackupGuard Wall</title>
<style>
html,body{margin:0;min-height:100%;font-family:system-ui,Arial,sans-serif;color:#fff}
body{transition:background .5s;display:flex;flex-direction:column;min-height:100vh}
body.GREEN{background:#0b6b2e} body.AMBER{background:#b36b00}
body.RED{background:#a31515} body.OFFLINE{background:#4b2a8a}
body.RED.flash{animation:fl 1s infinite}
@keyframes fl{50%{background:#4a0707}}
#top{padding:3vh 4vw 1vh;text-align:center}
#head{font-size:max(28px,7vw);font-weight:800;line-height:1.05}
#sub{font-size:max(15px,2vw);opacity:.9;margin-top:1vh}
#grid{flex:1;display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));
 gap:2vw;padding:2vw 4vw;align-content:start}
.t{border-radius:18px;padding:max(14px,1.8vw);background:rgba(0,0,0,.28);border-left:14px solid #4dff88}
.t.AMBER{border-color:#ffc247} .t.RED{border-color:#ff6b6b;cursor:pointer}
.n{font-size:max(20px,2.8vw);font-weight:700}
.s{font-size:max(14px,1.5vw);opacity:.92;margin-top:.8vh}
#bar{padding:1.4vh 4vw;display:flex;gap:1.5vw;align-items:center;flex-wrap:wrap;
 background:rgba(0,0,0,.35);font-size:max(13px,1.3vw)}
button{font-size:max(13px,1.3vw);padding:.6vw 1.4vw;border-radius:8px;border:0;cursor:pointer}
#hist span{display:inline-block;width:max(12px,1.5vw);height:max(12px,1.5vw);margin-right:.3vw;border-radius:3px}
</style>
<body class="AMBER">
<div id="top"><div id="head">CONNECTING...</div><div id="sub"></div></div>
<div id="grid"></div>
<div id="bar">
 <button id="en">Enable sound</button>
 <button id="ackall">Acknowledge all</button>
 <span>14 nights: <span id="hist"></span></span>
 <span id="upd" style="margin-left:auto"></span>
</div>
<script>
var S = null, audio = null, beep = null, beepMode = "", fails = 0, who = "";
function el(tag, cls, txt) {
  var e = document.createElement(tag);
  if (cls) e.className = cls;
  if (txt != null) e.textContent = txt;
  return e;
}
function fmt(ts) {
  if (!ts) return "never";
  return new Date(ts).toLocaleString([], {dateStyle: "short", timeStyle: "short"});
}
function ago(h) {
  if (h == null) return "no backup yet";
  if (h < 1) return Math.round(h * 60) + " min ago";
  return h.toFixed(1) + " h ago";
}
function getWho() {
  try { who = who || localStorage.getItem("who") || ""; } catch (e) {}
  if (!who) {
    who = prompt("Your name for the acknowledgement log:") || "operator";
    try { localStorage.setItem("who", who); } catch (e) {}
  }
  return who;
}
function ack(vol) {
  fetch("ack?vol=" + encodeURIComponent(vol) + "&by=" + encodeURIComponent(getWho())).then(poll);
}
document.getElementById("en").onclick = function () {
  audio = new (window.AudioContext || window.webkitAudioContext)();
  this.textContent = "Sound armed";
};
document.getElementById("ackall").onclick = function () { ack("all"); };

function render() {
  var b = document.body, head = document.getElementById("head"), sub = document.getElementById("sub");
  var grid = document.getElementById("grid");
  if (fails >= 3 || !S) {
    b.className = "OFFLINE";
    head.textContent = "API UNREACHABLE";
    sub.textContent = "Status unknown - do not assume backups are safe";
    return;
  }
  b.className = S.overall + (S.overall === "RED" && S.alarm !== "none" ? " flash" : "");
  var counts = {GREEN: 0, AMBER: 0, RED: 0};
  S.vols.forEach(function (v) { counts[v.state]++; });
  head.textContent = S.overall === "GREEN" ? "ALL BACKUPS PROTECTED"
    : S.overall === "AMBER" ? "ATTENTION: BACKUP RETRYING / LATE" : "BACKUP FAILURE";
  sub.textContent = S.vols.length + " volumes - " + counts.GREEN + " protected, "
    + counts.AMBER + " warning, " + counts.RED + " at risk"
    + (S.alarm === "escalated" ? " - ESCALATED, NOBODY ACKNOWLEDGED" : "");
  grid.textContent = "";
  S.vols.forEach(function (v) {
    var t = el("div", "t " + v.state);
    t.appendChild(el("div", "n", v.name));
    var label = v.state === "GREEN" ? "Protected" : v.state === "AMBER" ? "Warning - retrying or late"
      : (v.escalated ? "FAILED - ESCALATED" : "FAILED");
    t.appendChild(el("div", "s", label));
    t.appendChild(el("div", "s", "Last good: " + fmt(v.last_success) + " (" + ago(v.age_h) + ")"));
    if (v.acked && v.state === "RED")
      t.appendChild(el("div", "s", "Acked by " + (v.acked_by || "?") + " at " + fmt(v.acked_at)));
    if (v.state === "RED" && !v.acked) {
      t.appendChild(el("div", "s", "Tap to acknowledge"));
      t.onclick = function () { ack(v.id); };
    }
    grid.appendChild(t);
  });
  var h = document.getElementById("hist"); h.textContent = "";
  (S.hist || []).forEach(function (ok) {
    var s = document.createElement("span");
    s.style.background = ok ? "#4dff88" : "#ff6b6b"; h.appendChild(s);
  });
  document.getElementById("upd").textContent = "Updated " + new Date().toLocaleTimeString();
}
function buzz() {
  var mode = (S && fails < 3) ? S.alarm : "none";
  if (mode === "none") { if (beep) { clearInterval(beep); beep = null; beepMode = ""; } return; }
  if (!audio) return;
  if (beep && beepMode === mode) return;
  if (beep) clearInterval(beep);
  beepMode = mode;
  var esc = mode === "escalated";
  beep = setInterval(function () {
    var o = audio.createOscillator();
    o.type = "square"; o.frequency.value = esc ? 3200 : 2000;
    o.connect(audio.destination); o.start(); o.stop(audio.currentTime + (esc ? 0.15 : 0.25));
  }, esc ? 300 : 700);
}
function poll() {
  fetch("status?hb=1").then(function (r) { return r.json(); })
    .then(function (j) { S = j; fails = 0; render(); buzz(); })
    .catch(function () { fails++; render(); buzz(); });
}
poll(); setInterval(poll, 10000);
</script>
'''

# ================================================================== helpers
def utc():
    return datetime.datetime.utcnow()

def isoz(d):
    return d.isoformat() + "Z"

def make_zip(files):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for name, body in files.items():
            z.writestr(name, body)
    return buf.getvalue()

def clients():
    return dict(
        sts=boto3.client("sts", region_name=REGION),
        ec2=boto3.client("ec2", region_name=REGION),
        ec2dr=boto3.client("ec2", region_name=DR_REGION),
        ddb=boto3.client("dynamodb", region_name=REGION),
        sns=boto3.client("sns", region_name=REGION),
        lam=boto3.client("lambda", region_name=REGION),
        ev=boto3.client("events", region_name=REGION),
    )

def upsert_lambda(lam, name, role, handler, files, env):
    code = make_zip(files)
    try:
        lam.create_function(FunctionName=name, Runtime="python3.12", Role=role, Handler=handler,
                            Code={"ZipFile": code}, Timeout=30, Environment={"Variables": env})
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
        lam.update_function_code(FunctionName=name, ZipFile=code)
        lam.get_waiter("function_updated_v2").wait(FunctionName=name)
        lam.update_function_configuration(FunctionName=name, Handler=handler,
                                          Environment={"Variables": env})
        lam.get_waiter("function_updated_v2").wait(FunctionName=name)
    lam.get_waiter("function_active_v2").wait(FunctionName=name)
    return lam.get_function(FunctionName=name)["Configuration"]["FunctionArn"]

def allow(lam, fn, sid, principal, source_arn=None, action="lambda:InvokeFunction", **extra):
    kw = dict(FunctionName=fn, StatementId=sid, Action=action, Principal=principal, **extra)
    if source_arn:
        kw["SourceArn"] = source_arn
    try:
        lam.add_permission(**kw)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise

def tagval(v, key):
    return next((t["Value"] for t in v.get("Tags", []) if t["Key"] == key), None)

def find_volumes(ec2):
    out = {}
    for v in ec2.describe_volumes(Filters=[{"Name": "tag:Project", "Values": ["BackupGuard"]}])["Volumes"]:
        out[tagval(v, "Name") or v["VolumeId"]] = v["VolumeId"]
    return out

def find_topic(sns):
    for page in sns.get_paginator("list_topics").paginate():
        for t in page["Topics"]:
            if t["TopicArn"].endswith(":" + TOPIC):
                return t["TopicArn"]

def function_url(lam):
    try:
        return lam.get_function_url_config(FunctionName=FN_WEB)["FunctionUrl"]
    except ClientError:
        return None

def schedule(c, rule, expr, fn_arn, fn_name, sid):
    c["ev"].put_rule(Name=rule, ScheduleExpression=expr, State="ENABLED")
    c["ev"].put_targets(Rule=rule, Targets=[{"Id": "1", "Arn": fn_arn}])
    allow(c["lam"], fn_name, sid, "events.amazonaws.com", c["ev"].describe_rule(Name=rule)["Arn"])

# =================================================================== deploy
def deploy(email):
    c = clients()
    acct = c["sts"].get_caller_identity()["Account"]
    role = "arn:aws:iam::%s:role/LabRole" % acct
    print("Account", acct, "- using", role)

    try:
        c["ddb"].create_table(TableName=TABLE,
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}], BillingMode="PAY_PER_REQUEST")
        c["ddb"].get_waiter("table_exists").wait(TableName=TABLE)
        print("Created table", TABLE)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise

    topic = c["sns"].create_topic(Name=TOPIC)["TopicArn"]
    if email:
        c["sns"].subscribe(TopicArn=topic, Protocol="email", Endpoint=email)
        print("Confirm the subscription email sent to", email)

    # demo volumes (web-01, db-01, app-01), tagged for nightly backup
    vols = find_volumes(c["ec2"])
    if "BackupGuard-demo" in vols:                      # upgrade from v1
        vid = vols.pop("BackupGuard-demo")
        c["ec2"].create_tags(Resources=[vid], Tags=[{"Key": "Name", "Value": "web-01"}])
        vols["web-01"] = vid
    new_ids = []
    for name in DEMO_VOLUMES:
        if name not in vols:
            vid = c["ec2"].create_volume(AvailabilityZone=REGION + "a", Size=1, VolumeType="gp3",
                TagSpecifications=[{"ResourceType": "volume", "Tags": [TAG,
                    {"Key": "Name", "Value": name}]}])["VolumeId"]
            vols[name] = vid
            new_ids.append(vid)
            print("Created demo volume", name, vid)
    if new_ids:
        c["ec2"].get_waiter("volume_available").wait(VolumeIds=new_ids)
    c["ec2"].create_tags(Resources=list(vols.values()), Tags=[{"Key": "Backup", "Value": "nightly"}])

    # seed one status row per volume (so the wall shows tiles immediately)
    for name, vid in vols.items():
        try:
            c["ddb"].put_item(TableName=TABLE, ConditionExpression="attribute_not_exists(id)",
                Item={"id": {"S": "vol#" + vid}, "volume": {"S": vid}, "name": {"S": name},
                      "status": {"S": "UNKNOWN"}, "retries": {"N": "0"},
                      "acked": {"BOOL": False}, "created": {"S": isoz(utc())}})
        except ClientError as e:
            if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise

    env = {"TABLE": TABLE, "TOPIC_ARN": topic}
    cm = {"common.py": COMMON}
    snap = upsert_lambda(c["lam"], FN_SNAP, role, "make_snapshot.handler",
                         {"make_snapshot.py": MAKE_SNAPSHOT}, env)
    rec = upsert_lambda(c["lam"], FN_REC, role, "record_result.handler",
                        dict(cm, **{"record_result.py": RECORD_RESULT}), env)
    wd = upsert_lambda(c["lam"], FN_WATCH, role, "watchdog.handler",
                       dict(cm, **{"watchdog.py": WATCHDOG}), env)
    upsert_lambda(c["lam"], FN_WEB, role, "device.handler",
                  dict(cm, **{"device.py": DEVICE, "oled.html": OLED_HTML, "wall.html": WALL_HTML}), env)

    schedule(c, RULE_SCHED, "cron(0 20 * * ? *)", snap, FN_SNAP, "bg-sched")
    schedule(c, RULE_WATCH, "rate(5 minutes)", wd, FN_WATCH, "bg-watch")
    pattern = {"source": ["aws.ec2"], "detail-type": ["EBS Snapshot Notification"],
               "detail": {"event": ["createSnapshot"]}}
    c["ev"].put_rule(Name=RULE_EVT, EventPattern=json.dumps(pattern), State="ENABLED")
    c["ev"].put_targets(Rule=RULE_EVT, Targets=[{"Id": "1", "Arn": rec}])
    allow(c["lam"], FN_REC, "bg-events", "events.amazonaws.com", c["ev"].describe_rule(Name=RULE_EVT)["Arn"])

    try:
        c["lam"].create_function_url_config(FunctionName=FN_WEB, AuthType="NONE")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise
    allow(c["lam"], FN_WEB, "bg-url", "*", action="lambda:InvokeFunctionUrl", FunctionUrlAuthType="NONE")
    try:
        allow(c["lam"], FN_WEB, "bg-url2", "*", InvokedViaFunctionUrl=True)
    except TypeError:
        pass

    url = function_url(c["lam"])
    print("\nDONE.\n  Virtual OLED :", url, "\n  NOC wall     :", url + "wall")
    print("Next: python3 deploy.py test-ok   (go green now)   or   snapshot-now   (real snapshots)")

# ==================================================================== tests
def vol_map():
    return find_volumes(boto3.client("ec2", region_name=REGION))

def pick(vols, name):
    if name not in vols:
        raise SystemExit("Unknown volume %r. Choose from: %s" % (name, ", ".join(sorted(vols))))
    return vols[name]

def invoke_record(result, vol):
    lam = boto3.client("lambda", region_name=REGION)
    vols = vol_map()
    names = sorted(vols) if vol == "all" else [vol]
    for n in names:
        vid = pick(vols, n)
        evt = {"detail": {"event": "createSnapshot", "result": result,
                          "source": "arn:aws:ec2::%s:volume/%s" % (REGION, vid)}}
        lam.invoke(FunctionName=FN_REC, Payload=json.dumps(evt).encode())
        print("Sent simulated %s for %s" % (result, n))

def test_red(vol):
    ddb = boto3.client("dynamodb", region_name=REGION)
    vid = pick(vol_map(), vol)
    n = utc()
    ddb.update_item(TableName=TABLE, Key={"id": {"S": "vol#" + vid}},
        UpdateExpression="SET #s=:s, retries=:r, failed_since=:f, red_since=:rs, acked=:a "
                         "REMOVE escalated_sent, acked_by, acked_at",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": {"S": "FAILED"}, ":r": {"N": "3"},
            ":f": {"S": isoz(n - datetime.timedelta(minutes=40))},
            ":rs": {"S": isoz(n - datetime.timedelta(minutes=35))}, ":a": {"BOOL": False}})
    print("%s is now RED + escalated (retries exhausted). Escalation email within 5 minutes." % vol)

def snapshot_now():
    r = boto3.client("lambda", region_name=REGION).invoke(FunctionName=FN_SNAP)
    print("Snapshots started:", r["Payload"].read().decode(),
          "\nSuccess events arrive in a few minutes and turn tiles green.")

# ================================================================= teardown
def teardown():
    c = clients()
    def quiet(fn, *a, **k):
        try:
            fn(*a, **k)
        except Exception as e:
            print("  skip:", str(e)[:100])
    for rule in (RULE_SCHED, RULE_EVT, RULE_WATCH):
        quiet(c["ev"].remove_targets, Rule=rule, Ids=["1"])
        quiet(c["ev"].delete_rule, Name=rule)
    quiet(c["lam"].delete_function_url_config, FunctionName=FN_WEB)
    for fn in (FN_SNAP, FN_REC, FN_WEB, FN_WATCH):
        quiet(c["lam"].delete_function, FunctionName=fn)
    quiet(c["ddb"].delete_table, TableName=TABLE)
    t = find_topic(c["sns"])
    if t:
        quiet(c["sns"].delete_topic, TopicArn=t)
    for ec2 in (c["ec2"], c["ec2dr"]):
        for s in ec2.describe_snapshots(OwnerIds=["self"],
                Filters=[{"Name": "tag:Project", "Values": ["BackupGuard"]}])["Snapshots"]:
            quiet(ec2.delete_snapshot, SnapshotId=s["SnapshotId"])
    for vid in find_volumes(c["ec2"]).values():
        quiet(c["ec2"].delete_volume, VolumeId=vid)
    print("Teardown finished.")

# ===================================================================== main
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["deploy", "url", "snapshot-now", "test-fail", "test-red",
                                    "test-ok", "teardown"])
    ap.add_argument("--email")
    ap.add_argument("--vol")
    a = ap.parse_args()
    if a.cmd == "deploy":
        deploy(a.email)
    elif a.cmd == "url":
        u = function_url(boto3.client("lambda", region_name=REGION))
        print(u, "\n" + u + "wall" if u else "")
    elif a.cmd == "snapshot-now":
        snapshot_now()
    elif a.cmd == "test-fail":
        invoke_record("failed", a.vol or "web-01")
    elif a.cmd == "test-red":
        test_red(a.vol or "web-01")
    elif a.cmd == "test-ok":
        invoke_record("succeeded", a.vol or "all")
    elif a.cmd == "teardown":
        teardown()
