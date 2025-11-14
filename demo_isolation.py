#!/usr/bin/env python3
"""
CockroachDB Isolation Levels Demo
Demonstrates SERIALIZABLE vs READ_COMMITTED isolation levels
"""

import psycopg2
import sys
import time
import argparse
import threading
from datetime import datetime
from psycopg2.extras import RealDictCursor

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.columns import Columns
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("⚠️  Install 'rich' for better visualization: pip3 install rich")

CONN_STR = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
console = Console() if RICH_AVAILABLE else None

def get_connection():
    """Get database connection"""
    return psycopg2.connect(CONN_STR)

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

def setup_accounts_table():
    """Setup accounts table for isolation demo"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Drop and recreate table
    cursor.execute("DROP TABLE IF EXISTS defaultdb.accounts CASCADE")
    cursor.execute("""
        CREATE TABLE defaultdb.accounts (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            balance INT
        )
    """)
    
    # Insert test accounts
    cursor.execute("INSERT INTO defaultdb.accounts VALUES (1, 'Alice', 1000)")
    cursor.execute("INSERT INTO defaultdb.accounts VALUES (2, 'Bob', 1000)")
    cursor.execute("INSERT INTO defaultdb.accounts VALUES (3, 'Charlie', 1000)")
    
    conn.commit()
    cursor.close()
    conn.close()

def print_accounts(title=""):
    """Print current account balances"""
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM defaultdb.accounts ORDER BY id")
    accounts = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if RICH_AVAILABLE:
        table = Table(title=title or "Account Balances", box=box.ROUNDED)
        table.add_column("ID", style="cyan")
        table.add_column("Name", style="magenta")
        table.add_column("Balance", justify="right", style="green")
        
        total = 0
        for acc in accounts:
            table.add_row(str(acc['id']), acc['name'], f"${acc['balance']}")
            total += acc['balance']
        
        table.add_row("", "[bold]TOTAL[/bold]", f"[bold]${total}[/bold]")
        console.print(table)
        return total
    else:
        print(f"\n{title or 'Account Balances'}")
        print(f"{'ID':<5} {'Name':<15} {'Balance':<10}")
        print('-'*35)
        total = 0
        for acc in accounts:
            print(f"{acc['id']:<5} {acc['name']:<15} ${acc['balance']:<10}")
            total += acc['balance']
        print('-'*35)
        print(f"{'TOTAL':<20} ${total:<10}")
        return total

# Global counters for transaction results
tx_results = {
    'success': 0,
    'retries': 0,
    'conflicts': 0
}
tx_lock = threading.Lock()

def transaction_worker(isolation_level, tx_id, from_account, to_account, amount, delay=0):
    """Worker thread to execute a transaction"""
    conn = get_connection()
    
    try:
        # Set autocommit mode temporarily to set session variable
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"SET default_transaction_isolation = '{isolation_level}'")
        
        # Turn off autocommit - transaction will auto-start on first query
        conn.autocommit = False
        cursor = conn.cursor()
        
        if RICH_AVAILABLE:
            console.print(f"[yellow]TX{tx_id}:[/yellow] Started ({isolation_level})")
        else:
            print(f"TX{tx_id}: Started ({isolation_level})")
        
        # Read balance
        cursor.execute(f"SELECT balance FROM defaultdb.accounts WHERE id = {from_account}")
        from_balance = cursor.fetchone()[0]
        
        if RICH_AVAILABLE:
            console.print(f"[yellow]TX{tx_id}:[/yellow] Read account {from_account} balance: ${from_balance}")
        else:
            print(f"TX{tx_id}: Read account {from_account} balance: ${from_balance}")
        
        # Simulate some processing time
        time.sleep(delay)
        
        # Check sufficient funds
        if from_balance < amount:
            if RICH_AVAILABLE:
                console.print(f"[red]TX{tx_id}:[/red] Insufficient funds")
            else:
                print(f"TX{tx_id}: Insufficient funds")
            conn.rollback()
            return
        
        # Perform transfer
        cursor.execute(f"UPDATE defaultdb.accounts SET balance = balance - {amount} WHERE id = {from_account}")
        cursor.execute(f"UPDATE defaultdb.accounts SET balance = balance + {amount} WHERE id = {to_account}")
        
        if RICH_AVAILABLE:
            console.print(f"[yellow]TX{tx_id}:[/yellow] Transferring ${amount} from account {from_account} to {to_account}")
        else:
            print(f"TX{tx_id}: Transferring ${amount} from account {from_account} to {to_account}")
        
        # Commit transaction
        conn.commit()
        
        with tx_lock:
            tx_results['success'] += 1
        
        if RICH_AVAILABLE:
            console.print(f"[green]TX{tx_id}:[/green] ✓ Committed successfully")
        else:
            print(f"TX{tx_id}: ✓ Committed successfully")
        
    except psycopg2.extensions.TransactionRollbackError as e:
        # Serialization failure - retry needed
        conn.rollback()
        with tx_lock:
            tx_results['retries'] += 1
        
        if RICH_AVAILABLE:
            console.print(f"[red]TX{tx_id}:[/red] ⚠️  Retry required (serialization conflict)")
        else:
            print(f"TX{tx_id}: ⚠️  Retry required (serialization conflict)")
        
    except Exception as e:
        conn.rollback()
        with tx_lock:
            tx_results['conflicts'] += 1
        
        if RICH_AVAILABLE:
            console.print(f"[red]TX{tx_id}:[/red] ❌ Error: {e}")
        else:
            print(f"TX{tx_id}: ❌ Error: {e}")
    
    finally:
        cursor.close()
        conn.close()

def demo_serializable():
    """Demo SERIALIZABLE isolation level"""
    print_header(
        "SERIALIZABLE Isolation Level",
        "Two concurrent transactions transferring from the same account"
    )
    
    setup_accounts_table()
    
    if RICH_AVAILABLE:
        console.print("\n[cyan]Initial State:[/cyan]")
    else:
        print("\nInitial State:")
    print_accounts()
    
    if RICH_AVAILABLE:
        console.print("\n[cyan]Starting concurrent transactions...[/cyan]\n")
    else:
        print("\nStarting concurrent transactions...\n")
    
    # Reset counters
    global tx_results
    tx_results = {'success': 0, 'retries': 0, 'conflicts': 0}
    
    # Create two threads trying to transfer from account 1
    t1 = threading.Thread(target=transaction_worker, 
                         args=('serializable', 1, 1, 2, 300, 0.5))
    t2 = threading.Thread(target=transaction_worker, 
                         args=('serializable', 2, 1, 3, 400, 0.5))
    
    # Start both transactions at roughly the same time
    t1.start()
    t2.start()
    
    # Wait for both to complete
    t1.join()
    t2.join()
    
    time.sleep(0.5)
    
    if RICH_AVAILABLE:
        console.print("\n[cyan]Final State:[/cyan]")
    else:
        print("\nFinal State:")
    total = print_accounts()
    
    if RICH_AVAILABLE:
        console.print("\n[bold cyan]Results:[/bold cyan]")
        console.print(f"  • Successful commits: [green]{tx_results['success']}[/green]")
        console.print(f"  • Retries required: [yellow]{tx_results['retries']}[/yellow]")
        console.print(f"  • Total preserved: [green]${total}[/green] (should be $3000)")
        console.print("\n[bold cyan]Explanation:[/bold cyan]")
        console.print("  • SERIALIZABLE prevents write skew")
        console.print("  • One transaction succeeds, other gets retry error")
        console.print("  • Application can retry failed transaction")
        console.print("  • Total balance is always consistent")
    else:
        print("\nResults:")
        print(f"  Successful commits: {tx_results['success']}")
        print(f"  Retries required: {tx_results['retries']}")
        print(f"  Total preserved: ${total} (should be $3000)")

def demo_read_committed():
    """Demo READ_COMMITTED isolation level"""
    print_header(
        "READ_COMMITTED Isolation Level",
        "Two concurrent transactions transferring from the same account"
    )
    
    setup_accounts_table()
    
    if RICH_AVAILABLE:
        console.print("\n[cyan]Initial State:[/cyan]")
    else:
        print("\nInitial State:")
    print_accounts()
    
    if RICH_AVAILABLE:
        console.print("\n[cyan]Starting concurrent transactions...[/cyan]\n")
    else:
        print("\nStarting concurrent transactions...\n")
    
    # Reset counters
    global tx_results
    tx_results = {'success': 0, 'retries': 0, 'conflicts': 0}
    
    # Create two threads trying to transfer from account 1
    t1 = threading.Thread(target=transaction_worker, 
                         args=('read committed', 1, 1, 2, 300, 0.5))
    t2 = threading.Thread(target=transaction_worker, 
                         args=('read committed', 2, 1, 3, 400, 0.5))
    
    # Start both transactions at roughly the same time
    t1.start()
    t2.start()
    
    # Wait for both to complete
    t1.join()
    t2.join()
    
    time.sleep(0.5)
    
    if RICH_AVAILABLE:
        console.print("\n[cyan]Final State:[/cyan]")
    else:
        print("\nFinal State:")
    total = print_accounts()
    
    if RICH_AVAILABLE:
        console.print("\n[bold cyan]Results:[/bold cyan]")
        console.print(f"  • Successful commits: [green]{tx_results['success']}[/green]")
        console.print(f"  • Retries required: [yellow]{tx_results['retries']}[/yellow]")
        console.print(f"  • Total preserved: [{'green' if total == 3000 else 'red'}]${total}[/{'green' if total == 3000 else 'red'}]")
        console.print("\n[bold cyan]Explanation:[/bold cyan]")
        console.print("  • READ_COMMITTED allows both transactions to proceed")
        console.print("  • Both read initial balance ($1000) and proceed")
        console.print("  • May result in negative balance or overdraft")
        console.print("  • Application must implement additional checks")
    else:
        print("\nResults:")
        print(f"  Successful commits: {tx_results['success']}")
        print(f"  Retries required: {tx_results['retries']}")
        print(f"  Total preserved: ${total}")

def compare_isolation_levels():
    """Compare SERIALIZABLE vs READ_COMMITTED side by side"""
    print_header("Isolation Level Comparison", "Running both scenarios")
    
    if RICH_AVAILABLE:
        console.print("\n[bold cyan]Scenario:[/bold cyan] Two concurrent transactions transfer from the same account")
        console.print("  • Transaction 1: Transfer $300 from Alice → Bob")
        console.print("  • Transaction 2: Transfer $400 from Alice → Charlie")
        console.print("  • Alice starts with $1000")
        console.print("\n" + "="*70 + "\n")
    else:
        print("\nScenario: Two concurrent transactions transfer from the same account")
        print("  Transaction 1: Transfer $300 from Alice → Bob")
        print("  Transaction 2: Transfer $400 from Alice → Charlie")
        print("  Alice starts with $1000\n")
    
    # Run SERIALIZABLE
    print_header("Test 1: SERIALIZABLE (Default)")
    setup_accounts_table()
    tx_results_ser = run_concurrent_transfers('serializable')
    total_ser = print_accounts("SERIALIZABLE Result")
    
    print("\n" + "="*70 + "\n")
    
    # Run READ_COMMITTED
    print_header("Test 2: READ_COMMITTED")
    setup_accounts_table()
    tx_results_rc = run_concurrent_transfers('read committed')
    total_rc = print_accounts("READ_COMMITTED Result")
    
    # Summary comparison
    if RICH_AVAILABLE:
        console.print("\n" + "="*70 + "\n")
        table = Table(title="Comparison Summary", box=box.DOUBLE)
        table.add_column("Metric", style="cyan")
        table.add_column("SERIALIZABLE", style="green")
        table.add_column("READ_COMMITTED", style="yellow")
        
        table.add_row("Successful Commits", 
                     str(tx_results_ser['success']), 
                     str(tx_results_rc['success']))
        table.add_row("Retry Errors", 
                     str(tx_results_ser['retries']), 
                     str(tx_results_rc['retries']))
        table.add_row("Total Balance", 
                     f"${total_ser}" + (" ✓" if total_ser == 3000 else " ❌"),
                     f"${total_rc}" + (" ✓" if total_rc == 3000 else " ❌"))
        table.add_row("Consistency", 
                     "✓ Guaranteed" if total_ser == 3000 else "❌ Violated",
                     "✓ Maintained" if total_rc == 3000 else "❌ May violate")
        
        console.print(table)
        
        console.print("\n[bold cyan]Key Takeaways:[/bold cyan]")
        console.print("  • SERIALIZABLE: Prevents conflicts, requires retries, guarantees consistency")
        console.print("  • READ_COMMITTED: Higher concurrency, may need application-level checks")
        console.print("  • Choose based on: Consistency requirements vs. Performance needs")
    else:
        print("\n=== Comparison Summary ===")
        print(f"SERIALIZABLE: {tx_results_ser['success']} commits, {tx_results_ser['retries']} retries, Total: ${total_ser}")
        print(f"READ_COMMITTED: {tx_results_rc['success']} commits, {tx_results_rc['retries']} retries, Total: ${total_rc}")

def run_concurrent_transfers(isolation_level):
    """Helper to run concurrent transfers and return results"""
    global tx_results
    tx_results = {'success': 0, 'retries': 0, 'conflicts': 0}
    
    t1 = threading.Thread(target=transaction_worker, 
                         args=(isolation_level, 1, 1, 2, 300, 0.5))
    t2 = threading.Thread(target=transaction_worker, 
                         args=(isolation_level, 2, 1, 3, 400, 0.5))
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    time.sleep(0.5)
    return tx_results.copy()

def main():
    parser = argparse.ArgumentParser(description="CockroachDB Isolation Levels Demo")
    parser.add_argument('--mode', choices=['serializable', 'read_committed'], 
                       help='Run specific isolation level demo')
    parser.add_argument('--compare', action='store_true', 
                       help='Compare both isolation levels')
    parser.add_argument('--setup', action='store_true',
                       help='Just setup accounts table')
    
    args = parser.parse_args()
    
    try:
        if args.setup:
            setup_accounts_table()
            print("✓ Accounts table created and populated")
        elif args.mode == 'serializable':
            demo_serializable()
        elif args.mode == 'read_committed':
            demo_read_committed()
        elif args.compare:
            compare_isolation_levels()
        else:
            parser.print_help()
            print("\nExample usage:")
            print("  python3 demo_isolation.py --mode serializable")
            print("  python3 demo_isolation.py --mode read_committed")
            print("  python3 demo_isolation.py --compare")
            
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
