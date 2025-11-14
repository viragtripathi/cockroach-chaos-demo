#!/bin/bash
# Quick test script to verify all demo components work

set -e

echo "🧪 Testing CockroachDB Demo Scripts"
echo "===================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if cluster is running
echo -n "Checking if CockroachDB cluster is running... "
if docker ps | grep -q crdb-e1a; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
    echo "Please start the cluster first: ./run.sh"
    exit 1
fi

# Check Python dependencies
echo -n "Checking Python dependencies... "
python3 -c "import psycopg2, rich" 2>/dev/null
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${YELLOW}⚠${NC}"
    echo "Missing packages. Installing psycopg2-binary, rich, tabulate..."
    if pip3 install psycopg2-binary rich tabulate; then
        echo -e "${GREEN}✓ Packages installed${NC}"
    else
        echo -e "${RED}✗ Failed to install packages${NC}"
        echo "Please install manually: pip3 install psycopg2-binary rich tabulate"
        exit 1
    fi
fi

# Test database connection
echo -n "Testing database connection... "
docker exec crdb-e1a ./cockroach sql --insecure -e "SELECT 1" >/dev/null 2>&1
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
    echo "Cannot connect to database"
    exit 1
fi

# Test replication script
echo ""
echo "Testing replication demo script..."
python3 demo_replication.py --status
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Replication script works${NC}"
else
    echo -e "${RED}✗ Replication script failed${NC}"
    exit 1
fi

# Test isolation script setup
echo ""
echo "Testing isolation demo script..."
python3 demo_isolation.py --setup
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Isolation script works${NC}"
else
    echo -e "${RED}✗ Isolation script failed${NC}"
    exit 1
fi

# Check dashboards
echo ""
echo "Checking dashboard availability..."

echo -n "  Chaos Panel (http://localhost:8088)... "
curl -s http://localhost:8088 >/dev/null 2>&1
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${YELLOW}⚠ Not accessible${NC}"
fi

echo -n "  CockroachDB Admin (http://localhost:8080)... "
curl -s http://localhost:8080 >/dev/null 2>&1
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${YELLOW}⚠ Not accessible${NC}"
fi

echo -n "  HAProxy Stats (http://localhost:8404/stats)... "
curl -s http://localhost:8404/stats >/dev/null 2>&1
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${YELLOW}⚠ Not accessible${NC}"
fi

echo ""
echo -e "${GREEN}✓ All tests passed!${NC}"
echo ""
echo "You're ready to run the demo! Quick start:"
echo "  python3 demo_replication.py --all"
echo "  python3 demo_isolation.py --compare"
echo "  python3 demo_visual_monitoring.py"
echo ""
echo "See DEMO_SCRIPT.md for full demonstration guide"
