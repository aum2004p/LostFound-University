"""
LostFoundApp - ระบบของหาย-ของพบ (Serverless บน AWS)

Lambda ตัวเดียวทำ 3 หน้าที่:
  1) GET  /           -> ส่งหน้าเว็บ web/index.html จาก S3
  2) GET  /api/items  -> รายการที่ยังเปิดอยู่ (ไม่ส่งอีเมลออกไป)
     POST /api/items  -> แจ้งของหาย / แจ้งเก็บของได้ (แนบรูปได้)
  3) EventBridge (ไม่มี HTTP) -> ปิดรายการค้าง + ส่งสรุปให้ผู้ดูแล

Environment variables: BUCKET, TABLE, TOPIC_ARN, (ไม่บังคับ) EXPIRE_DAYS
"""
import base64
import json
import os
import re
import time
import traceback
import uuid
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.config import Config

BUCKET = os.environ.get("BUCKET", "")
TABLE = os.environ.get("TABLE", "lostfound-items")
TOPIC_ARN = os.environ.get("TOPIC_ARN", "")
REGION = os.environ.get("AWS_REGION", "us-east-1")

s3 = boto3.client("s3", region_name=REGION, config=Config(signature_version="s3v4"))
sns = boto3.client("sns", region_name=REGION)
table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE)

CATEGORIES = [
    "กระเป๋า/กระเป๋าสตางค์",
    "โทรศัพท์/อุปกรณ์อิเล็กทรอนิกส์",
    "กุญแจ",
    "บัตรนักศึกษา/เอกสาร",
    "เสื้อผ้า/เครื่องประดับ",
    "หนังสือ/เครื่องเขียน",
    "อื่น ๆ",
]
LOCATIONS = [
    "โรงอาหาร",
    "ห้องสมุด",
    "อาคารเรียน",
    "ลานจอดรถ",
    "สนามกีฬา",
    "หอพัก",
    "อื่น ๆ",
]
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
MAX_IMAGE_BYTES = 3 * 1024 * 1024


# ---------------------------------------------------------------- helpers
def _json_default(o):
    if isinstance(o, Decimal):
        return int(o) if o == int(o) else float(o)
    raise TypeError(f"not serializable: {type(o)}")


def respond(status, body, content_type="application/json; charset=utf-8"):
    if not isinstance(body, str):
        body = json.dumps(body, ensure_ascii=False, default=_json_default)
    return {
        "statusCode": status,
        "headers": {"Content-Type": content_type, "Cache-Control": "no-store"},
        "body": body,
    }


def scan_all(filter_expr=None):
    """Scan ทั้งตาราง (ข้อมูลโปรเจกต์มีไม่มาก) พร้อมรองรับ pagination"""
    kwargs = {}
    if filter_expr is not None:
        kwargs["FilterExpression"] = filter_expr
    items = []
    while True:
        r = table.scan(**kwargs)
        items.extend(r.get("Items", []))
        if "LastEvaluatedKey" not in r:
            return items
        kwargs["ExclusiveStartKey"] = r["LastEvaluatedKey"]


def public_view(item):
    """ข้อมูลที่ส่งให้หน้าเว็บ (ไม่มีอีเมล)"""
    out = {
        "item_id": item["item_id"],
        "type": item["type"],
        "title": item["title"],
        "category": item["category"],
        "location": item["location"],
        "description": item.get("description", ""),
        "created_at": int(item["created_at"]),
        "image_url": None,
    }
    key = item.get("image_key")
    if key:
        out["image_url"] = s3.generate_presigned_url(
            "get_object", Params={"Bucket": BUCKET, "Key": key}, ExpiresIn=3600
        )
    return out


def find_open(item_type, category, location):
    cond = (
        Attr("type").eq(item_type)
        & Attr("status").eq("open")
        & Attr("category").eq(category)
        & Attr("location").eq(location)
    )
    return scan_all(cond)


def ensure_subscription(email):
    """สมัคร email เข้า topic (ถ้ายังไม่มี) พร้อม filter ให้ได้เฉพาะเมลของตัวเอง"""
    paginator = sns.get_paginator("list_subscriptions_by_topic")
    for page in paginator.paginate(TopicArn=TOPIC_ARN):
        for sub in page["Subscriptions"]:
            if sub.get("Endpoint", "").lower() == email:
                return
    sns.subscribe(
        TopicArn=TOPIC_ARN,
        Protocol="email",
        Endpoint=email,
        Attributes={"FilterPolicy": json.dumps({"owner_email": [email]})},
    )


def notify(owner_email, subject, message):
    sns.publish(
        TopicArn=TOPIC_ARN,
        Subject=subject,
        Message=message,
        MessageAttributes={
            "owner_email": {"DataType": "String", "StringValue": owner_email}
        },
    )


# ---------------------------------------------------------------- handlers
def serve_page():
    obj = s3.get_object(Bucket=BUCKET, Key="web/index.html")
    html = obj["Body"].read().decode("utf-8")
    return respond(200, html, "text/html; charset=utf-8")


def list_items():
    items = scan_all(Attr("status").eq("open"))
    items.sort(key=lambda i: i["created_at"], reverse=True)
    return respond(200, {"items": [public_view(i) for i in items[:100]]})


def parse_image(data_url):
    """รับ data URL (jpeg/png) -> (bytes, นามสกุล, content-type)"""
    if "," not in data_url:
        raise ValueError("รูปแบบรูปภาพไม่ถูกต้อง")
    raw = base64.b64decode(data_url.split(",", 1)[1], validate=True)
    if len(raw) > MAX_IMAGE_BYTES:
        raise ValueError("รูปใหญ่เกิน 3 MB")
    if raw[:3] == b"\xff\xd8\xff":
        return raw, "jpg", "image/jpeg"
    if raw[:8] == b"\x89PNG\r\n\x1a\n":
        return raw, "png", "image/png"
    raise ValueError("รองรับเฉพาะรูป JPG หรือ PNG")


def create_item(event):
    body = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        raise ValueError("ข้อมูลที่ส่งมาไม่ถูกต้อง")

    item_type = data.get("type")
    title = str(data.get("title", "")).strip()[:100]
    category = data.get("category")
    location = data.get("location")
    description = str(data.get("description", "")).strip()[:500]
    email = str(data.get("email", "")).strip().lower()

    if item_type not in ("lost", "found"):
        raise ValueError("ประเภทรายการไม่ถูกต้อง")
    if not title:
        raise ValueError("กรุณากรอกชื่อสิ่งของ")
    if category not in CATEGORIES:
        raise ValueError("หมวดหมู่ไม่ถูกต้อง")
    if location not in LOCATIONS:
        raise ValueError("สถานที่ไม่ถูกต้อง")
    if not EMAIL_RE.match(email) or len(email) > 120:
        raise ValueError("อีเมลไม่ถูกต้อง")

    item_id = uuid.uuid4().hex
    item = {
        "item_id": item_id,
        "type": item_type,
        "title": title,
        "category": category,
        "location": location,
        "description": description,
        "email": email,
        "status": "open",
        "created_at": int(time.time()),
    }

    if data.get("image"):
        raw, ext, ctype = parse_image(data["image"])
        key = f"images/{item_id}.{ext}"
        s3.put_object(Bucket=BUCKET, Key=key, Body=raw, ContentType=ctype)
        item["image_key"] = key

    table.put_item(Item=item)

    if item_type == "lost":
        ensure_subscription(email)
        matches = find_open("found", category, location)
        return respond(
            200,
            {
                "ok": True,
                "item_id": item_id,
                "matches": [public_view(m) for m in matches],
            },
        )

    # item_type == "found": แจ้งเจ้าของที่แจ้งของหายหมวด+สถานที่ตรงกัน
    owners = {}
    for lost in find_open("lost", category, location):
        owners[lost["email"]] = lost
    for owner_email, lost in owners.items():
        message = (
            "มีคนแจ้งเก็บของที่อาจเป็นของที่คุณแจ้งหาย\n\n"
            f"ของที่คุณแจ้งหาย : {lost['title']}\n"
            f"ของที่มีคนเก็บได้ : {title}\n"
            f"หมวดหมู่ : {category}\n"
            f"สถานที่ : {location}\n"
            f"รายละเอียด : {description or '-'}\n\n"
            f"ติดต่อผู้เก็บได้ที่อีเมล : {email}\n"
        )
        notify(owner_email, "พบของที่อาจเป็นของคุณ", message)
    return respond(200, {"ok": True, "item_id": item_id, "notified": len(owners)})


def daily_job():
    """EventBridge เรียกทุกวัน: ปิดรายการค้างเกิน EXPIRE_DAYS วัน แล้วส่งสรุปให้ผู้ดูแล"""
    days = int(os.environ.get("EXPIRE_DAYS", "30"))
    now = int(time.time())
    cutoff = now - days * 86400

    expired = 0
    lost_open = 0
    found_open = 0
    for item in scan_all(Attr("status").eq("open")):
        if int(item["created_at"]) <= cutoff:
            table.update_item(
                Key={"item_id": item["item_id"]},
                UpdateExpression="SET #s = :e, expired_at = :t",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":e": "expired", ":t": now},
            )
            expired += 1
        elif item["type"] == "lost":
            lost_open += 1
        else:
            found_open += 1

    message = (
        "สรุปประจำวัน ระบบของหาย-ของพบ\n\n"
        f"ของหายที่ยังเปิดอยู่ : {lost_open} รายการ\n"
        f"ของที่เก็บได้ที่ยังเปิดอยู่ : {found_open} รายการ\n"
        f"ปิดรายการเพราะค้างเกิน {days} วัน : {expired} รายการ\n"
    )
    notify("admin", "สรุปประจำวัน ของหาย-ของพบ", message)
    print(message)
    return {"statusCode": 200, "body": json.dumps({"expired": expired})}


def lambda_handler(event, context):
    http = (event.get("requestContext") or {}).get("http")
    if not http:  # ไม่ใช่ HTTP = มาจาก EventBridge หรือปุ่ม Test
        return daily_job()

    method = http.get("method", "GET")
    path = http.get("path", "/")
    try:
        if method == "GET" and path in ("/", "/index.html"):
            return serve_page()
        if path == "/api/items" and method == "GET":
            return list_items()
        if path == "/api/items" and method == "POST":
            return create_item(event)
        return respond(404, {"error": "ไม่พบหน้านี้"})
    except ValueError as e:
        return respond(400, {"error": str(e)})
    except Exception:
        traceback.print_exc()  # ไปดูรายละเอียดที่ CloudWatch Logs
        return respond(500, {"error": "server error"})
