#!/usr/bin/env python3
"""
Real-time Visual Monitoring of CockroachDB Cluster
Shows live updates of nodes, transactions, and replication status
"""

import psycopg2
import sys
import time
import signal
from datetime import datetime
from psycopg2.extras import RealDictCursor

try:
    from rich.console import Console
    from rich.live import Live
    from rich.table import Table
    from rich.layout import Layout
    from rich.panel import Panel
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    print("❌ This script requires 'rich' library")
    print("Install: pip3 install rich")
    sys.exit(1)

CONN_STR = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
console = Console()

NODES = {
    1: {"name": "crdb-e1a", "region": "us-east-1", "zone": "a"},
    2: {"name": "crdb-e1b", "region": "us-east-1", "zone": "b"},
    3: {"name": "crdb-w2a", "region": "us-west-2", "zone": "a"},
    4: {"name": "crdb-w2b", "region": "us-west-2", "zone": "b"},
    5: {"name": "crdb-c1", "region": "us-central-1", "zone": "a"},
}

running = True

def signal_handler(sig, frame):
    global running
    running = False

signal.signal(signal.SIGINT, signal_handler)

def get_connection():
    """Get database connection"""
    try:
        return psycopg2.connect(CONN_STR, connect_timeout=3)
    except:
        return None

def ensure_demo_table():
    """Ensure demo_transactions table exists"""
    try:
        conn = get_connection()
        if not conn:
            return
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
    except:
        pass  # Silently fail for monitoring script

def generate_cluster_table():
    """Generate cluster status table"""
    table = Table(title="🪲 CockroachDB Cluster Status", box=box.HEAVY_EDGE)
    table.add_column("Node", style="cyan", no_wrap=True)
    table.add_column("Region", style="magenta")
    table.add_column("Status", style="green")
    table.add_column("Address", style="blue")
    
    conn = get_connection()
    if not conn:
        table.add_row("ERROR", "Connection Failed", "🔴", "-", "-")
        return table
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get node status
        cursor.execute("""
            SELECT 
                node_id,
                is_live,
                address
            FROM crdb_internal.gossip_nodes
            ORDER BY node_id
        """)
        nodes = cursor.fetchall()
        
        for node in nodes:
            node_id = node['node_id']
            node_info = NODES.get(node_id, {})
            
            status = "🟢 LIVE" if node['is_live'] else "🔴 DEAD"
            status_style = "green" if node['is_live'] else "red"
            
            table.add_row(
                f"n{node_id} ({node_info.get('name', 'unknown')})",
                node_info.get('region', 'unknown'),
                f"[{status_style}]{status}[/{status_style}]",
                node.get('address', 'unknown')
            )
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        table.add_row("ERROR", str(e), "🔴", "-", "-")
    
    return table

def generate_stats_table():
    """Generate statistics table"""
    table = Table(title="📊 Cluster Statistics", box=box.ROUNDED)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", justify="right", style="green")
    
    conn = get_connection()
    if not conn:
        table.add_row("Status", "[red]Disconnected[/red]")
        return table
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Live nodes
        cursor.execute("""
            SELECT count(*) as live_nodes 
            FROM crdb_internal.gossip_nodes 
            WHERE is_live = true
        """)
        live_nodes = cursor.fetchone()['live_nodes']
        table.add_row("Live Nodes", f"{live_nodes}/5")
        
        # Total transactions
        cursor.execute("SELECT count(*) as count FROM defaultdb.demo_transactions")
        tx_count = cursor.fetchone()['count']
        table.add_row("Total Transactions", str(tx_count))
        
        # Range count
        cursor.execute("""
            SELECT count(DISTINCT range_id) as range_count 
            FROM crdb_internal.ranges_no_leases
        """)
        ranges = cursor.fetchone()['range_count']
        table.add_row("Total Ranges", str(ranges))
        
        # User table ranges
        cursor.execute("""
            SELECT count(DISTINCT range_id) as range_count 
            FROM crdb_internal.ranges
            WHERE start_pretty LIKE '%/Table/%'
              AND range_id > 10
        """)
        demo_ranges = cursor.fetchone()['range_count']
        table.add_row("User Table Ranges", str(demo_ranges))
        
        # Replication factor
        cursor.execute("""
            SELECT array_length(replicas, 1) as rf
            FROM crdb_internal.ranges
            WHERE start_pretty LIKE '%/Table/%'
              AND range_id > 10
            LIMIT 1
        """)
        result = cursor.fetchone()
        rf = result['rf'] if result else 3
        table.add_row("Replication Factor", str(rf))
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        table.add_row("Error", str(e))
    
    return table

def generate_replication_table():
    """Generate replication distribution table"""
    table = Table(title="🔄 Replica Distribution", box=box.ROUNDED)
    table.add_column("Node", style="cyan")
    table.add_column("Region", style="magenta")
    table.add_column("Replica Count", justify="right", style="green")
    
    conn = get_connection()
    if not conn:
        table.add_row("ERROR", "Connection Failed", "-")
        return table
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT 
                node_id,
                count(*) as replica_count
            FROM (
                SELECT unnest(replicas) as node_id
                FROM crdb_internal.ranges
                WHERE start_pretty LIKE '%/Table/%'
                  AND range_id > 10
            )
            GROUP BY node_id
            ORDER BY node_id
        """)
        replicas = cursor.fetchall()
        
        for row in replicas:
            node_id = row['node_id']
            node_info = NODES.get(node_id, {})
            table.add_row(
                f"n{node_id} ({node_info.get('name', 'unknown')})",
                node_info.get('region', 'unknown'),
                str(row['replica_count'])
            )
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        table.add_row("ERROR", str(e), "-")
    
    return table

def generate_dashboard():
    """Generate complete dashboard layout"""
    layout = Layout()
    
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="main"),
        Layout(name="footer", size=3)
    )
    
    layout["main"].split_row(
        Layout(name="left"),
        Layout(name="right")
    )
    
    # Header
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    layout["header"].update(
        Panel(
            f"[bold cyan]CockroachDB Multi-Region Cluster Monitor[/bold cyan] | {timestamp}",
            border_style="cyan"
        )
    )
    
    # Left panel - Cluster status
    layout["left"].split_column(
        Layout(generate_cluster_table()),
        Layout(generate_replication_table())
    )
    
    # Right panel - Statistics
    layout["right"].update(generate_stats_table())
    
    # Footer
    layout["footer"].update(
        Panel(
            "[yellow]Press Ctrl+C to exit[/yellow] | "
            "[cyan]Updates every 2 seconds[/cyan] | "
            "[green]Chaos Panel: http://localhost:8088[/green]",
            border_style="yellow"
        )
    )
    
    return layout

def main():
    """Main monitoring loop"""
    ensure_demo_table()
    console.clear()
    
    try:
        with Live(generate_dashboard(), refresh_per_second=0.5, console=console) as live:
            while running:
                live.update(generate_dashboard())
                time.sleep(2)
    except KeyboardInterrupt:
        pass
    
    console.print("\n[yellow]Monitoring stopped[/yellow]")

if __name__ == "__main__":
    main()
