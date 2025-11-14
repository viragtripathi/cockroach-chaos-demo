#!/usr/bin/env python3
"""
CockroachDB Replication Demo Script
Visualizes data distribution and replication across nodes
"""

import psycopg2
import sys
import time
import argparse
from datetime import datetime
from psycopg2.extras import RealDictCursor

try:
    from rich.console import Console
    from rich.table import Table
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
    from rich.panel import Panel
    from rich.layout import Layout
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("⚠️  Install 'rich' for better visualization: pip3 install rich")

# Connection string
CONN_STR = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"

# Node information
NODES = {
    1: {"name": "crdb-e1a", "region": "us-east-1", "zone": "a", "color": "green"},
    2: {"name": "crdb-e1b", "region": "us-east-1", "zone": "b", "color": "green"},
    3: {"name": "crdb-w2a", "region": "us-west-2", "zone": "a", "color": "blue"},
    4: {"name": "crdb-w2b", "region": "us-west-2", "zone": "b", "color": "blue"},
    5: {"name": "crdb-c1", "region": "us-central-1", "zone": "a", "color": "yellow"},
}

console = Console() if RICH_AVAILABLE else None

def get_connection():
    """Get database connection with retry"""
    for i in range(3):
        try:
            return psycopg2.connect(CONN_STR)
        except Exception as e:
            if i == 2:
                raise
            time.sleep(1)

def print_header(title):
    """Print formatted header"""
    if RICH_AVAILABLE:
        console.print(Panel(f"[bold cyan]{title}[/bold cyan]", expand=False))
    else:
        print(f"\n{'='*60}")
        print(f"  {title}")
        print('='*60)

def get_cluster_status():
    """Get cluster health and node status"""
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get node liveness
    cursor.execute("""
        SELECT 
            node_id,
            address,
            locality,
            is_live
        FROM crdb_internal.gossip_nodes
        ORDER BY node_id
    """)
    nodes = cursor.fetchall()
    
    # Get transaction count
    cursor.execute("SELECT count(*) as count FROM defaultdb.demo_transactions")
    tx_count = cursor.fetchone()['count']
    
    cursor.close()
    conn.close()
    
    return nodes, tx_count

def show_status():
    """Show cluster status and transaction count"""
    print_header("Cluster Status")
    
    nodes, tx_count = get_cluster_status()
    
    if RICH_AVAILABLE:
        table = Table(title="Node Status", box=box.ROUNDED)
        table.add_column("Node", style="cyan", no_wrap=True)
        table.add_column("Region", style="magenta")
        table.add_column("Status", style="green")
        table.add_column("Address", style="blue")
        
        for node in nodes:
            node_id = node['node_id']
            node_info = NODES.get(node_id, {})
            status = "🟢 LIVE" if node['is_live'] else "🔴 DEAD"
            status_style = "green" if node['is_live'] else "red"
            
            table.add_row(
                f"Node {node_id} ({node_info.get('name', 'unknown')})",
                node_info.get('region', 'unknown'),
                f"[{status_style}]{status}[/{status_style}]",
                node.get('address', 'unknown')
            )
        
        console.print(table)
        console.print(f"\n[bold green]Total Transactions:[/bold green] {tx_count}")
    else:
        print(f"\n{'Node':<20} {'Region':<15} {'Status':<10} {'Address':<20}")
        print('-'*70)
        for node in nodes:
            node_id = node['node_id']
            node_info = NODES.get(node_id, {})
            status = "LIVE" if node['is_live'] else "DEAD"
            print(f"Node {node_id:<2} ({node_info.get('name', 'unknown'):<10}) "
                  f"{node_info.get('region', 'unknown'):<15} {status:<10} "
                  f"{node.get('address', 'unknown'):<20}")
        print(f"\nTotal Transactions: {tx_count}")

def insert_transactions(count=50):
    """Insert transactions with progress visualization"""
    print_header(f"Inserting {count} Transactions")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    if RICH_AVAILABLE:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            console=console
        ) as progress:
            task = progress.add_task("[cyan]Writing transactions...", total=count)
            
            for i in range(count):
                amount = (i + 1) * 10
                cursor.execute(
                    "INSERT INTO defaultdb.demo_transactions (ts, amount) VALUES (now(), %s)",
                    (amount,)
                )
                conn.commit()
                progress.update(task, advance=1)
                time.sleep(0.05)  # Small delay for visual effect
    else:
        print(f"Inserting {count} transactions...")
        for i in range(count):
            amount = (i + 1) * 10
            cursor.execute(
                "INSERT INTO defaultdb.demo_transactions (ts, amount) VALUES (now(), %s)",
                (amount,)
            )
            conn.commit()
            if (i + 1) % 10 == 0:
                print(f"  Inserted {i + 1}/{count}...")
    
    cursor.close()
    conn.close()
    
    if RICH_AVAILABLE:
        console.print(f"\n[bold green]✓[/bold green] Successfully inserted {count} transactions")
    else:
        print(f"\n✓ Successfully inserted {count} transactions")

def show_distribution():
    """Show data distribution across nodes"""
    print_header("Data Distribution Across Nodes")
    
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get table ID for demo_transactions
    cursor.execute("""
        SELECT 
            'Table/' || table_id::STRING as table_prefix
        FROM crdb_internal.tables
        WHERE name = 'demo_transactions' AND database_name = 'defaultdb'
    """)
    result = cursor.fetchone()
    table_prefix = result['table_prefix'] if result else None
    
    # Get range distribution for demo_transactions table
    cursor.execute("""
        SELECT 
            range_id,
            start_pretty,
            end_pretty,
            replicas,
            replica_localities,
            lease_holder
        FROM crdb_internal.ranges
        WHERE start_pretty LIKE '%/Table/%'
        ORDER BY range_id
        LIMIT 10
    """)
    ranges = cursor.fetchall()
    
    # Get replica counts per node (across all user tables)
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
    replica_counts = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    if RICH_AVAILABLE:
        # Show replica distribution per node
        table1 = Table(title="Replicas Per Node", box=box.ROUNDED)
        table1.add_column("Node", style="cyan")
        table1.add_column("Region", style="magenta")
        table1.add_column("Replica Count", justify="right", style="green")
        
        for row in replica_counts:
            node_id = row['node_id']
            node_info = NODES.get(node_id, {})
            table1.add_row(
                f"Node {node_id} ({node_info.get('name', 'unknown')})",
                node_info.get('region', 'unknown'),
                str(row['replica_count'])
            )
        
        console.print(table1)
        
        # Show range distribution
        table2 = Table(title=f"Range Distribution (showing {len(ranges)} ranges)", box=box.ROUNDED)
        table2.add_column("Range ID", style="cyan")
        table2.add_column("Replicas (Nodes)", style="green")
        table2.add_column("Leaseholder", style="yellow")
        table2.add_column("Regions", style="magenta")
        
        for r in ranges:
            replicas_str = ", ".join([f"n{node}" for node in r['replicas']])
            leaseholder = f"n{r['lease_holder']}"
            
            # Extract regions from replica_localities
            regions = set()
            if r['replica_localities']:
                for loc in r['replica_localities']:
                    if 'region=' in loc:
                        region = loc.split('region=')[1].split(',')[0]
                        regions.add(region)
            regions_str = ", ".join(sorted(regions))
            
            table2.add_row(
                str(r['range_id']),
                replicas_str,
                leaseholder,
                regions_str
            )
        
        console.print(table2)
        
        console.print("\n[bold cyan]Key Insights:[/bold cyan]")
        console.print("  • Each range has 3 replicas (default replication factor)")
        console.print("  • Replicas are distributed across different nodes and regions")
        console.print("  • Leaseholder handles reads for that range")
        console.print("  • Data survives failure of any 1 node (quorum = 2/3)")
        
    else:
        print("\n--- Replicas Per Node ---")
        print(f"{'Node':<25} {'Region':<15} {'Replica Count':<15}")
        print('-'*60)
        for row in replica_counts:
            node_id = row['node_id']
            node_info = NODES.get(node_id, {})
            print(f"Node {node_id:<2} ({node_info.get('name', 'unknown'):<15}) "
                  f"{node_info.get('region', 'unknown'):<15} {row['replica_count']:<15}")
        
        print(f"\n--- Range Distribution (showing {len(ranges)} ranges) ---")
        for r in ranges:
            replicas_str = ", ".join([f"n{node}" for node in r['replicas']])
            print(f"Range {r['range_id']}: Replicas=[{replicas_str}], "
                  f"Leaseholder=n{r['lease_holder']}")

def show_real_time_monitoring(duration=10):
    """Show real-time transaction monitoring"""
    print_header(f"Real-Time Monitoring ({duration}s)")
    
    if not RICH_AVAILABLE:
        print("Install 'rich' for real-time visualization: pip3 install rich")
        return
    
    console.print("[yellow]Monitoring transaction count every second...[/yellow]\n")
    
    for i in range(duration):
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("SELECT count(*) as count FROM defaultdb.demo_transactions")
        count = cursor.fetchone()['count']
        
        cursor.execute("""
            SELECT count(DISTINCT node_id) as live_nodes 
            FROM crdb_internal.gossip_nodes 
            WHERE is_live = true
        """)
        live_nodes = cursor.fetchone()['live_nodes']
        
        cursor.close()
        conn.close()
        
        timestamp = datetime.now().strftime("%H:%M:%S")
        console.print(f"[{timestamp}] Transactions: [bold green]{count}[/bold green] | "
                     f"Live Nodes: [bold cyan]{live_nodes}/5[/bold cyan]")
        
        time.sleep(1)

def main():
    parser = argparse.ArgumentParser(description="CockroachDB Replication Demo")
    parser.add_argument('--status', action='store_true', help='Show cluster status')
    parser.add_argument('--insert', type=int, metavar='N', help='Insert N transactions')
    parser.add_argument('--distribution', action='store_true', help='Show data distribution')
    parser.add_argument('--monitor', type=int, metavar='SECONDS', help='Monitor in real-time')
    parser.add_argument('--all', action='store_true', help='Run full demo sequence')
    
    args = parser.parse_args()
    
    try:
        if args.status:
            show_status()
        elif args.insert:
            insert_transactions(args.insert)
            show_status()
        elif args.distribution:
            show_distribution()
        elif args.monitor:
            show_real_time_monitoring(args.monitor)
        elif args.all:
            show_status()
            print("\n")
            insert_transactions(50)
            print("\n")
            show_distribution()
        else:
            parser.print_help()
            
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
