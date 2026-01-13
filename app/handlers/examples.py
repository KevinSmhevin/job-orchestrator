"""
Example job handlers for testing and demonstration.

These handlers demonstrate various patterns and can be used for testing
the job orchestrator system.
"""
import time
import random
from datetime import datetime
from typing import Any

from app.handlers.registry import registry


# ─────────────────────────────────────────────────────────────────
# Simple Handlers
# ─────────────────────────────────────────────────────────────────

@registry.register("hello_world")
def hello_world(payload: dict) -> None:
    """
    Simple hello world handler.
    
    Payload:
        name (str, optional): Name to greet. Defaults to "World"
    
    Example:
        {"name": "Alice"}
    """
    name = payload.get("name", "World")
    print(f"👋 Hello, {name}!")


@registry.register("echo")
def echo(payload: dict) -> None:
    """
    Echo back the payload.
    
    Payload:
        Any dictionary
    
    Example:
        {"message": "test", "count": 42}
    """
    print(f"🔊 Echo: {payload}")


@registry.register("noop")
def noop(payload: dict) -> None:
    """
    No-op handler that does nothing.
    
    Useful for testing job infrastructure without actual work.
    """
    print("⚪ No-op handler executed")


# ─────────────────────────────────────────────────────────────────
# Time-based Handlers (for timeout/heartbeat testing)
# ─────────────────────────────────────────────────────────────────

@registry.register("sleep")
def sleep_handler(payload: dict) -> None:
    """
    Sleep for a specified duration.
    
    Payload:
        duration (int): Seconds to sleep
    
    Example:
        {"duration": 5}
    """
    duration = payload.get("duration", 1)
    print(f"😴 Sleeping for {duration} seconds...")
    time.sleep(duration)
    print(f"⏰ Woke up after {duration} seconds!")


@registry.register("slow_task")
def slow_task(payload: dict) -> None:
    """
    Simulate a slow-running task with progress updates.
    
    Useful for testing heartbeat mechanisms and long-running jobs.
    
    Payload:
        duration (int): Total duration in seconds. Defaults to 30.
        update_interval (int): Seconds between progress updates. Defaults to 5.
    
    Example:
        {"duration": 60, "update_interval": 10}
    """
    duration = payload.get("duration", 30)
    update_interval = payload.get("update_interval", 5)
    
    print(f"🐌 Starting slow task ({duration}s total)")
    
    elapsed = 0
    while elapsed < duration:
        time.sleep(min(update_interval, duration - elapsed))
        elapsed += min(update_interval, duration - elapsed)
        progress = (elapsed / duration) * 100
        print(f"📊 Progress: {progress:.1f}% ({elapsed}/{duration}s)")
    
    print(f"✅ Slow task completed!")


# ─────────────────────────────────────────────────────────────────
# Failure Handlers (for retry testing)
# ─────────────────────────────────────────────────────────────────

@registry.register("always_fail")
def always_fail(payload: dict) -> None:
    """
    Always fails with an error.
    
    Useful for testing retry mechanisms and error handling.
    
    Payload:
        message (str, optional): Error message
    
    Example:
        {"message": "Custom error"}
    """
    message = payload.get("message", "This task always fails!")
    print(f"💥 Failing with error: {message}")
    raise Exception(message)


@registry.register("random_fail")
def random_fail(payload: dict) -> None:
    """
    Randomly succeeds or fails.
    
    Useful for testing retry logic with intermittent failures.
    
    Payload:
        success_rate (float): Probability of success (0.0 to 1.0). Defaults to 0.5.
    
    Example:
        {"success_rate": 0.7}
    """
    success_rate = payload.get("success_rate", 0.5)
    roll = random.random()
    
    if roll < success_rate:
        print(f"✅ Random success! (rolled {roll:.2f} < {success_rate})")
    else:
        print(f"💥 Random failure! (rolled {roll:.2f} >= {success_rate})")
        raise Exception(f"Random failure (rolled {roll:.2f})")


@registry.register("fail_until_attempt")
def fail_until_attempt(payload: dict) -> None:
    """
    Fails until a specific attempt number, then succeeds.
    
    Useful for testing retry logic with eventual success.
    
    Payload:
        succeed_on_attempt (int): Attempt number to succeed on. Defaults to 3.
        current_attempt (int): Current attempt number (tracked in payload)
    
    Example:
        {"succeed_on_attempt": 3, "current_attempt": 1}
    
    Note:
        The worker should increment current_attempt in the payload on each retry.
    """
    succeed_on = payload.get("succeed_on_attempt", 3)
    current = payload.get("current_attempt", 1)
    
    print(f"🎯 Attempt {current} (will succeed on attempt {succeed_on})")
    
    if current >= succeed_on:
        print(f"✅ Success on attempt {current}!")
    else:
        print(f"💥 Failing... need {succeed_on - current} more attempt(s)")
        raise Exception(f"Not ready yet (attempt {current}/{succeed_on})")


# ─────────────────────────────────────────────────────────────────
# Real-world Examples
# ─────────────────────────────────────────────────────────────────

@registry.register("send_email")
def send_email(payload: dict) -> None:
    """
    Simulate sending an email.
    
    Payload:
        to (str): Recipient email address
        subject (str): Email subject
        body (str, optional): Email body
        cc (list[str], optional): CC recipients
    
    Example:
        {
            "to": "user@example.com",
            "subject": "Welcome!",
            "body": "Welcome to our service",
            "cc": ["admin@example.com"]
        }
    """
    to = payload["to"]
    subject = payload["subject"]
    body = payload.get("body", "")
    cc = payload.get("cc", [])
    
    print(f"\n📧 Sending Email")
    print(f"   To: {to}")
    if cc:
        print(f"   CC: {', '.join(cc)}")
    print(f"   Subject: {subject}")
    print(f"   Body: {body[:100]}{'...' if len(body) > 100 else ''}")
    
    # Simulate sending
    time.sleep(1)
    
    print(f"✅ Email sent successfully!")


@registry.register("send_sms")
def send_sms(payload: dict) -> None:
    """
    Simulate sending an SMS.
    
    Payload:
        phone (str): Phone number
        message (str): SMS message
    
    Example:
        {
            "phone": "+1234567890",
            "message": "Your verification code is 123456"
        }
    """
    phone = payload["phone"]
    message = payload["message"]
    
    print(f"\n📱 Sending SMS")
    print(f"   To: {phone}")
    print(f"   Message: {message}")
    
    # Simulate sending
    time.sleep(0.5)
    
    print(f"✅ SMS sent successfully!")


@registry.register("generate_report")
def generate_report(payload: dict) -> None:
    """
    Simulate generating a report.
    
    Payload:
        report_type (str): Type of report (sales, users, activity)
        start_date (str): Start date (ISO format)
        end_date (str): End date (ISO format)
        user_id (str, optional): User requesting the report
        format (str, optional): Output format (pdf, csv, excel). Defaults to pdf.
    
    Example:
        {
            "report_type": "sales",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "user_id": "user123",
            "format": "pdf"
        }
    """
    report_type = payload["report_type"]
    start_date = payload["start_date"]
    end_date = payload["end_date"]
    user_id = payload.get("user_id", "system")
    output_format = payload.get("format", "pdf")
    
    print(f"\n📊 Generating {report_type.upper()} Report")
    print(f"   Period: {start_date} to {end_date}")
    print(f"   Format: {output_format}")
    print(f"   Requested by: {user_id}")
    
    # Simulate report generation
    print("   📈 Collecting data...")
    time.sleep(2)
    print("   🔢 Processing metrics...")
    time.sleep(2)
    print("   📄 Formatting report...")
    time.sleep(1)
    
    print(f"✅ Report generated successfully!")
    print(f"   📁 Saved as: reports/{report_type}_{start_date}_{end_date}.{output_format}")


@registry.register("process_webhook")
def process_webhook(payload: dict) -> None:
    """
    Simulate processing a webhook event.
    
    Payload:
        source (str): Webhook source (github, stripe, slack, etc.)
        event_type (str): Type of event
        data (dict): Event data
    
    Example:
        {
            "source": "github",
            "event_type": "push",
            "data": {
                "repository": "myrepo",
                "branch": "main",
                "commits": 3
            }
        }
    """
    source = payload["source"]
    event_type = payload["event_type"]
    data = payload.get("data", {})
    
    print(f"\n🔗 Processing Webhook")
    print(f"   Source: {source}")
    print(f"   Event: {event_type}")
    print(f"   Data: {data}")
    
    # Simulate processing
    time.sleep(1)
    
    print(f"✅ Webhook processed successfully!")


@registry.register("backup_database")
def backup_database(payload: dict) -> None:
    """
    Simulate a database backup operation.
    
    Payload:
        database (str): Database name
        backup_type (str): full or incremental. Defaults to full.
        compress (bool): Whether to compress backup. Defaults to True.
    
    Example:
        {
            "database": "production",
            "backup_type": "full",
            "compress": true
        }
    """
    database = payload["database"]
    backup_type = payload.get("backup_type", "full")
    compress = payload.get("compress", True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print(f"\n💾 Starting Database Backup")
    print(f"   Database: {database}")
    print(f"   Type: {backup_type}")
    print(f"   Compress: {compress}")
    
    # Simulate backup process
    print("   🔒 Acquiring lock...")
    time.sleep(1)
    print("   📦 Dumping data...")
    time.sleep(3)
    
    if compress:
        print("   🗜️  Compressing backup...")
        time.sleep(2)
    
    print(f"✅ Backup completed successfully!")
    print(f"Saved as: backups/{database}_{backup_type}_{timestamp}.sql{''if compress else ''}")


@registry.register("cleanup_old_files")
def cleanup_old_files(payload: dict) -> None:
    """
    Simulate cleaning up old files.
    
    Payload:
        directory (str): Directory to clean
        days_old (int): Delete files older than this many days
        dry_run (bool, optional): If true, only simulate. Defaults to False.
    
    Example:
        {
            "directory": "/tmp/uploads",
            "days_old": 30,
            "dry_run": false
        }
    """
    directory = payload["directory"]
    days_old = payload["days_old"]
    dry_run = payload.get("dry_run", False)
    
    print(f"\n🧹 Cleaning Up Old Files")
    print(f"   Directory: {directory}")
    print(f"   Threshold: {days_old} days")
    print(f"   Dry run: {dry_run}")
    
    # Simulate file scanning and deletion
    num_files = random.randint(10, 100)
    print(f"   🔍 Scanning files...")
    time.sleep(1)
    print(f"   📋 Found {num_files} old files")
    
    if not dry_run:
        print(f"   🗑️  Deleting files...")
        time.sleep(2)
        print(f"✅ Deleted {num_files} files")
    else:
        print(f"✅ Dry run complete (would delete {num_files} files)")


# ─────────────────────────────────────────────────────────────────
# Validation Examples
# ─────────────────────────────────────────────────────────────────

@registry.register("validated_handler")
def validated_handler(payload: dict) -> None:
    """
    Handler with payload validation.
    
    Demonstrates input validation before processing.
    
    Payload:
        user_id (str): User ID (required)
        amount (float): Amount (required, must be > 0)
        currency (str, optional): Currency code. Defaults to USD.
    
    Example:
        {
            "user_id": "user123",
            "amount": 99.99,
            "currency": "USD"
        }
    """
    # Validate required fields
    if "user_id" not in payload:
        raise ValueError("Missing required field: user_id")
    
    if "amount" not in payload:
        raise ValueError("Missing required field: amount")
    
    user_id = payload["user_id"]
    amount = payload["amount"]
    currency = payload.get("currency", "USD")
    
    # Validate types and values
    if not isinstance(amount, (int, float)):
        raise ValueError(f"amount must be a number, got {type(amount)}")
    
    if amount <= 0:
        raise ValueError(f"amount must be positive, got {amount}")
    
    print(f"✅ Processing payment:")
    print(f"   User: {user_id}")
    print(f"   Amount: {amount} {currency}")
    
    time.sleep(1)
    print(f"✅ Payment processed!")


# ─────────────────────────────────────────────────────────────────
# Debug/Testing Handlers
# ─────────────────────────────────────────────────────────────────

@registry.register("test_all_features")
def test_all_features(payload: dict) -> None:
    """
    Test handler that exercises multiple features.
    
    Useful for comprehensive testing.
    
    Payload:
        duration (int, optional): How long to run. Defaults to 5.
        should_fail (bool, optional): Whether to fail at end. Defaults to False.
    """
    duration = payload.get("duration", 5)
    should_fail = payload.get("should_fail", False)
    
    print(f"🧪 Test handler starting")
    print(f"   Duration: {duration}s")
    print(f"   Will fail: {should_fail}")
    
    # Simulate work with progress
    for i in range(duration):
        time.sleep(1)
        print(f"   ⏱️  {i + 1}/{duration}s")
    
    if should_fail:
        print("💥 Test failure!")
        raise Exception("Intentional test failure")
    
    print("✅ Test completed successfully!")


@registry.register("memory_heavy")
def memory_heavy(payload: dict) -> None:
    """
    Simulate a memory-intensive task.
    
    Payload:
        mb_to_allocate (int): Megabytes to allocate. Defaults to 10.
    
    Example:
        {"mb_to_allocate": 100}
    
    Warning:
        Use with caution in production!
    """
    mb = payload.get("mb_to_allocate", 10)
    
    print(f"💾 Allocating {mb}MB of memory...")
    
    # Allocate memory (list of 1MB strings)
    data = []
    for i in range(mb):
        data.append("x" * (1024 * 1024))  # 1MB string
        if (i + 1) % 10 == 0:
            print(f"   📊 Allocated {i + 1}MB")
    
    time.sleep(2)
    
    print(f"✅ Memory test complete (allocated {mb}MB)")
    
    # Memory will be freed when function returns


# ─────────────────────────────────────────────────────────────────
# Print registered handlers on import
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from app.handlers.registry import registry as reg
    
    print("\n📋 Registered Example Handlers:\n")
    for name, desc in reg.list_with_descriptions().items():
        print(f"  • {name}")
        print(f"    {desc.split('.')[0]}")
        print()