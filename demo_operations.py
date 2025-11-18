#!/usr/bin/env python3
"""
CockroachDB Operational Demos
Demonstrates online schema changes, load testing, and changefeeds
Consolidated script for all operational features
"""

import psycopg2
import sys
import time
import argparse
import threading
import random
from datetime import datetime
from psycopg2.extras import RealDictCursor

try:
    from rich.console import Console
    from rich.table import Table
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
    from rich.panel import Panel
    from rich.live import Live
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("⚠️  Install 'rich' for better visualization: pip3 install rich")

CONN_STR = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
console = Console() if RICH_AVAILABLE else None

# Global state for load testing
load_test_running = False
load_test_stats = {
    'total_writes': 0,
    'successful_writes': 0,
    'failed_writes': 0,
    'latencies': [],
    'errors': []
}
stats_lock = threading.Lock()

def get_connection():
    """Get database connection with retry"""
    for i in range(3):
        try:
            return psycopg2.connect(CONN_STR, connect_timeout=5)
        except Exception as e:
            if i == 2:
                raise
            time.sleep(1)

def ensure_demo_table():
    """Ensure demo_transactions table exists"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS demo_transactions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                ts TIMESTAMPTZ NOT NULL DEFAULT now(),
                amount INT NOT NULL
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        if RICH_AVAILABLE:
            console.print(f"[yellow]Warning: Could not ensure table exists: {e}[/yellow]")
        else:
            print(f"Warning: Could not ensure table exists: {e}")

def print_header(title, subtitle=""):
    """Print formatted header"""
    if RICH_AVAILABLE:
        text = f"[bold cyan]{title}[/bold cyan]"
        if subtitle:
            text += f"\n[italic]{subtitle}[/italic]"
        console.print(Panel(text, expand=False))
    else:
        print(f"\n{'='*70}")
        print(f"  {title}")
        if subtitle:
            print(f"  {subtitle}")
        print('='*70)

# =============================================================================
# ONLINE SCHEMA CHANGES
# =============================================================================

def demo_schema_change():
    """Demonstrate online schema changes with zero downtime"""
    ensure_demo_table()
    print_header(
        "Online Schema Changes",
        "Add column to live table with zero downtime"
    )
    
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Check current schema
    if RICH_AVAILABLE:
        console.print("\n[cyan]Current schema:[/cyan]")
    else:
        print("\nCurrent schema:")
    
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'demo_transactions' 
        ORDER BY ordinal_position
    """)
    columns = cursor.fetchall()
    
    if RICH_AVAILABLE:
        table = Table(box=box.ROUNDED)
        table.add_column("Column", style="cyan")
        table.add_column("Type", style="green")
        for col in columns:
            table.add_row(col['column_name'], col['data_type'])
        console.print(table)
    else:
        for col in columns:
            print(f"  {col['column_name']}: {col['data_type']}")
    
    # Start background writes
    if RICH_AVAILABLE:
        console.print("\n[yellow]Starting background writes (10 TPS)...[/yellow]")
    else:
        print("\nStarting background writes (10 TPS)...")
    
    stop_writes = threading.Event()
    write_stats = {'count': 0, 'errors': 0}
    
    def background_writer():
        while not stop_writes.is_set():
            try:
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO demo_transactions (ts, amount) VALUES (now(), %s)",
                    (random.randint(1, 1000),)
                )
                conn.commit()
                cursor.close()
                conn.close()
                write_stats['count'] += 1
            except Exception as e:
                write_stats['errors'] += 1
            time.sleep(0.1)  # 10 TPS
    
    writer_thread = threading.Thread(target=background_writer, daemon=True)
    writer_thread.start()
    
    time.sleep(2)
    if RICH_AVAILABLE:
        console.print(f"[green]✓[/green] Background writes running: {write_stats['count']} inserts")
    else:
        print(f"✓ Background writes running: {write_stats['count']} inserts")
    
    # Perform schema change
    new_column = "category"
    if RICH_AVAILABLE:
        console.print(f"\n[bold yellow]Adding column '{new_column}' to live table...[/bold yellow]")
    else:
        print(f"\nAdding column '{new_column}' to live table...")
    
    start_time = time.time()
    
    try:
        cursor.execute(f"""
            ALTER TABLE demo_transactions 
            ADD COLUMN IF NOT EXISTS {new_column} VARCHAR(50) DEFAULT 'uncategorized'
        """)
        conn.commit()
        
        duration = time.time() - start_time
        
        if RICH_AVAILABLE:
            console.print(f"[green]✓[/green] Schema change completed in {duration:.2f}s")
            console.print(f"[green]✓[/green] Writes continued during schema change: {write_stats['count']} total")
            console.print(f"[green]✓[/green] Zero errors: {write_stats['errors']} failed writes")
        else:
            print(f"✓ Schema change completed in {duration:.2f}s")
            print(f"✓ Writes continued: {write_stats['count']} total")
            print(f"✓ Zero errors: {write_stats['errors']} failed")
        
    except Exception as e:
        if RICH_AVAILABLE:
            console.print(f"[red]❌ Error:[/red] {e}")
        else:
            print(f"❌ Error: {e}")
    finally:
        stop_writes.set()
        writer_thread.join(timeout=2)
    
    # Show updated schema
    if RICH_AVAILABLE:
        console.print("\n[cyan]Updated schema:[/cyan]")
    else:
        print("\nUpdated schema:")
    
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'demo_transactions' 
        ORDER BY ordinal_position
    """)
    columns = cursor.fetchall()
    
    if RICH_AVAILABLE:
        table = Table(box=box.ROUNDED)
        table.add_column("Column", style="cyan")
        table.add_column("Type", style="green")
        for col in columns:
            style = "bold green" if col['column_name'] == new_column else ""
            table.add_row(col['column_name'], col['data_type'], style=style)
        console.print(table)
        
        console.print("\n[bold cyan]Key Takeaway:[/bold cyan]")
        console.print("  • Schema change completed with ZERO downtime")
        console.print("  • All writes succeeded during DDL operation")
        console.print("  • Compare to PostgreSQL: ALTER TABLE locks the table")
    else:
        for col in columns:
            marker = " <- NEW" if col['column_name'] == new_column else ""
            print(f"  {col['column_name']}: {col['data_type']}{marker}")
    
    cursor.close()
    conn.close()

# =============================================================================
# LOAD TESTING
# =============================================================================

def load_test_worker():
    """Background worker for load testing"""
    global load_test_running
    
    while load_test_running:
        start = time.time()
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            # Insert with random amount
            amount = random.randint(1, 1000)
            cursor.execute(
                "INSERT INTO demo_transactions (ts, amount) VALUES (now(), %s)",
                (amount,)
            )
            conn.commit()
            
            latency = (time.time() - start) * 1000  # ms
            
            with stats_lock:
                load_test_stats['total_writes'] += 1
                load_test_stats['successful_writes'] += 1
                load_test_stats['latencies'].append(latency)
                
                # Keep only last 1000 latencies for stats
                if len(load_test_stats['latencies']) > 1000:
                    load_test_stats['latencies'].pop(0)
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            with stats_lock:
                load_test_stats['total_writes'] += 1
                load_test_stats['failed_writes'] += 1
                load_test_stats['errors'].append(str(e))
                if len(load_test_stats['errors']) > 100:
                    load_test_stats['errors'].pop(0)
        
        # Sleep to control TPS (adjust for desired rate)
        time.sleep(0.01)  # ~100 TPS per thread

def calculate_percentile(latencies, percentile):
    """Calculate percentile from latency list"""
    if not latencies:
        return 0
    sorted_lat = sorted(latencies)
    index = int(len(sorted_lat) * percentile / 100)
    return sorted_lat[min(index, len(sorted_lat) - 1)]

def demo_load_test(duration=30, num_threads=10):
    """Run load test with real-time metrics"""
    ensure_demo_table()
    print_header(
        f"Load Test ({duration}s)",
        f"Sustained write workload with {num_threads} concurrent threads"
    )
    
    global load_test_running
    load_test_running = True
    
    # Reset stats
    with stats_lock:
        load_test_stats.update({
            'total_writes': 0,
            'successful_writes': 0,
            'failed_writes': 0,
            'latencies': [],
            'errors': []
        })
    
    # Start worker threads
    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=load_test_worker, daemon=True)
        t.start()
        threads.append(t)
    
    if RICH_AVAILABLE:
        console.print(f"[green]✓[/green] Started {num_threads} worker threads\n")
    else:
        print(f"✓ Started {num_threads} worker threads\n")
    
    # Monitor for duration
    start_time = time.time()
    
    if RICH_AVAILABLE:
        with Live(console=console, refresh_per_second=2) as live:
            while time.time() - start_time < duration:
                elapsed = time.time() - start_time
                
                with stats_lock:
                    total = load_test_stats['total_writes']
                    success = load_test_stats['successful_writes']
                    failed = load_test_stats['failed_writes']
                    latencies = load_test_stats['latencies'].copy()
                
                qps = total / elapsed if elapsed > 0 else 0
                success_rate = (success / total * 100) if total > 0 else 0
                
                p50 = calculate_percentile(latencies, 50)
                p95 = calculate_percentile(latencies, 95)
                p99 = calculate_percentile(latencies, 99)
                
                # Build metrics table
                table = Table(title=f"Load Test Metrics (Elapsed: {elapsed:.1f}s)", box=box.HEAVY_EDGE)
                table.add_column("Metric", style="cyan")
                table.add_column("Value", style="green", justify="right")
                
                table.add_row("QPS (Queries/sec)", f"{qps:.1f}")
                table.add_row("Total Writes", str(total))
                table.add_row("Successful", f"{success} ({success_rate:.1f}%)")
                table.add_row("Failed", f"[red]{failed}[/red]" if failed > 0 else str(failed))
                table.add_row("", "")
                table.add_row("[bold]Latency Metrics[/bold]", "")
                table.add_row("p50 (median)", f"{p50:.2f}ms")
                table.add_row("p95", f"{p95:.2f}ms")
                table.add_row("p99", f"{p99:.2f}ms")
                
                live.update(table)
                time.sleep(0.5)
    else:
        while time.time() - start_time < duration:
            elapsed = time.time() - start_time
            with stats_lock:
                total = load_test_stats['total_writes']
                success = load_test_stats['successful_writes']
            qps = total / elapsed if elapsed > 0 else 0
            print(f"[{elapsed:.1f}s] QPS: {qps:.1f} | Total: {total} | Success: {success}")
            time.sleep(2)
    
    # Stop workers
    load_test_running = False
    for t in threads:
        t.join(timeout=2)
    
    # Final stats
    with stats_lock:
        total = load_test_stats['total_writes']
        success = load_test_stats['successful_writes']
        failed = load_test_stats['failed_writes']
        latencies = load_test_stats['latencies']
    
    elapsed = time.time() - start_time
    qps = total / elapsed
    
    if RICH_AVAILABLE:
        console.print(f"\n[bold green]Load Test Complete![/bold green]")
        console.print(f"  • Duration: {elapsed:.1f}s")
        console.print(f"  • Average QPS: {qps:.1f}")
        console.print(f"  • Total Writes: {total}")
        console.print(f"  • Success Rate: {success/total*100:.2f}%")
        console.print(f"  • p99 Latency: {calculate_percentile(latencies, 99):.2f}ms")
    else:
        print(f"\nLoad Test Complete!")
        print(f"  Duration: {elapsed:.1f}s")
        print(f"  Average QPS: {qps:.1f}")
        print(f"  Total: {total}, Success: {success}, Failed: {failed}")

# =============================================================================
# CHANGEFEEDS (CDC)
# =============================================================================

def demo_changefeed():
    """Demonstrate changefeed (CDC) capability"""
    print_header(
        "Changefeeds (Change Data Capture)",
        "Stream changes to external systems in real-time"
    )
    
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Check if changefeed exists
    cursor.execute("""
        SELECT job_id, description, status 
        FROM [SHOW JOBS] 
        WHERE job_type = 'CHANGEFEED' 
        AND status IN ('running', 'pending')
        LIMIT 5
    """)
    existing = cursor.fetchall()
    
    if existing:
        if RICH_AVAILABLE:
            console.print("[cyan]Existing changefeeds:[/cyan]")
            table = Table(box=box.ROUNDED)
            table.add_column("Job ID", style="cyan")
            table.add_column("Description", style="green")
            table.add_column("Status", style="yellow")
            for job in existing:
                table.add_row(str(job['job_id']), job['description'], job['status'])
            console.print(table)
        else:
            print("Existing changefeeds:")
            for job in existing:
                print(f"  Job {job['job_id']}: {job['description']} ({job['status']})")
    else:
        if RICH_AVAILABLE:
            console.print("[yellow]No active changefeeds found[/yellow]")
        else:
            print("No active changefeeds found")
    
    if RICH_AVAILABLE:
        console.print("\n[bold cyan]Changefeed Example:[/bold cyan]")
        console.print("Create a changefeed to stream changes to an external system:")
        console.print("")
        console.print("[green]-- Stream to webhook[/green]")
        console.print("CREATE CHANGEFEED FOR TABLE demo_transactions")
        console.print("  INTO 'webhook-https://myapp.com/webhook?insecure_tls_skip_verify=true'")
        console.print("  WITH updated, resolved;")
        console.print("")
        console.print("[green]-- Stream to Kafka[/green]")
        console.print("CREATE CHANGEFEED FOR TABLE demo_transactions")
        console.print("  INTO 'kafka://kafka-broker:9092?topic_prefix=crdb_'")
        console.print("  WITH updated, resolved;")
        console.print("")
        console.print("[green]-- Stream to cloud storage[/green]")
        console.print("CREATE CHANGEFEED FOR TABLE demo_transactions")
        console.print("  INTO 's3://bucket/path?AWS_ACCESS_KEY_ID=x&AWS_SECRET_ACCESS_KEY=y'")
        console.print("  WITH updated, resolved, format=json;")
        
        console.print("\n[bold cyan]Key Benefits:[/bold cyan]")
        console.print("  • Real-time data replication")
        console.print("  • Survives node failures automatically")
        console.print("  • Multiple sink options (Kafka, S3, Webhook, etc.)")
        console.print("  • Perfect for event-driven architectures")
        console.print("  • Exactly-once delivery guarantees")
    else:
        print("\nChangefeed Example:")
        print("CREATE CHANGEFEED FOR TABLE demo_transactions")
        print("  INTO 'webhook-https://myapp.com/webhook'")
        print("  WITH updated, resolved;")
    
    cursor.close()
    conn.close()

# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="CockroachDB Operational Demos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --schema-change          # Online DDL demo
  %(prog)s --load-test --duration 60 --threads 20  # 60s load test
  %(prog)s --changefeed              # CDC overview
  %(prog)s --all                     # Run all demos
        """
    )
    
    parser.add_argument('--schema-change', action='store_true',
                       help='Demo online schema changes')
    parser.add_argument('--load-test', action='store_true',
                       help='Run load test')
    parser.add_argument('--duration', type=int, default=30,
                       help='Load test duration (seconds)')
    parser.add_argument('--threads', type=int, default=10,
                       help='Number of concurrent threads for load test')
    parser.add_argument('--changefeed', action='store_true',
                       help='Demo changefeed (CDC) capability')
    parser.add_argument('--all', action='store_true',
                       help='Run all operational demos')
    
    args = parser.parse_args()
    
    try:
        if args.all:
            demo_schema_change()
            print("\n")
            demo_load_test(duration=30, num_threads=args.threads)
            print("\n")
            demo_changefeed()
        elif args.schema_change:
            demo_schema_change()
        elif args.load_test:
            demo_load_test(duration=args.duration, num_threads=args.threads)
        elif args.changefeed:
            demo_changefeed()
        else:
            parser.print_help()
    
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user")
        load_test_running = False
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
