#!/usr/bin/env bash
#
# Warehouse Development Environment Script
#
# This script helps manage the test database containers for federated query testing.
#
# Usage:
#   ./scripts/warehouse-dev.sh [command]
#
# Commands:
#   start      Start all test database containers
#   stop       Stop all test database containers
#   restart    Restart all containers
#   status     Show status of all containers
#   logs       Show logs from all containers
#   reset      Stop, remove volumes, and start fresh
#   seed       Re-run seed scripts (containers must be running)
#   psql       Connect to test PostgreSQL
#   mysql      Connect to test MySQL
#   mongo      Connect to test MongoDB
#   mssql      Connect to test SQL Server

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.test-dbs.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}  Warehouse Dev Environment${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_docker() {
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed or not in PATH"
        exit 1
    fi
    if ! docker info &> /dev/null; then
        print_error "Docker daemon is not running"
        exit 1
    fi
}

cmd_start() {
    print_header
    echo "Starting test database containers..."
    docker compose -f "$COMPOSE_FILE" up -d
    
    echo ""
    echo "Waiting for containers to be healthy..."
    sleep 5
    
    # Wait for each service to be ready
    local services=("test-mysql" "test-postgres" "test-mongodb" "test-sqlserver" "minio")
    for service in "${services[@]}"; do
        echo -n "  Waiting for $service... "
        local retries=30
        while [ $retries -gt 0 ]; do
            if docker compose -f "$COMPOSE_FILE" ps "$service" 2>/dev/null | grep -q "healthy\|running"; then
                print_success "ready"
                break
            fi
            sleep 2
            retries=$((retries - 1))
        done
        if [ $retries -eq 0 ]; then
            print_warning "timeout (may still be starting)"
        fi
    done
    
    echo ""
    print_success "All containers started!"
    echo ""
    echo "Connection details:"
    echo "  PostgreSQL: localhost:15432 (testuser/testpass/testdb)"
    echo "  MySQL:      localhost:13306 (testuser/testpass/testdb)"
    echo "  MongoDB:    localhost:27018 (testuser/testpass/testdb)"
    echo "  SQL Server: localhost:11433 (sa/TestPass123!/testdb)"
    echo "  MinIO:      localhost:19000 (minioadmin/minioadmin)"
    echo "             Console: http://localhost:19001"
}

cmd_stop() {
    print_header
    echo "Stopping test database containers..."
    docker compose -f "$COMPOSE_FILE" down
    print_success "All containers stopped"
}

cmd_restart() {
    cmd_stop
    echo ""
    cmd_start
}

cmd_status() {
    print_header
    echo "Container status:"
    echo ""
    docker compose -f "$COMPOSE_FILE" ps
}

cmd_logs() {
    local service="${1:-}"
    if [ -n "$service" ]; then
        docker compose -f "$COMPOSE_FILE" logs -f "$service"
    else
        docker compose -f "$COMPOSE_FILE" logs -f
    fi
}

cmd_reset() {
    print_header
    print_warning "This will delete all data in test databases!"
    read -p "Are you sure? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Stopping containers and removing volumes..."
        docker compose -f "$COMPOSE_FILE" down -v
        echo ""
        cmd_start
    else
        echo "Cancelled"
    fi
}

cmd_seed() {
    print_header
    echo "Re-running seed scripts..."
    
    echo "  Seeding PostgreSQL..."
    docker compose -f "$COMPOSE_FILE" exec -T test-postgres psql -U testuser -d testdb < "$PROJECT_DIR/scripts/seed/postgres/01_init.sql"
    
    echo "  Seeding MySQL..."
    docker compose -f "$COMPOSE_FILE" exec -T test-mysql mysql -u testuser -ptestpass testdb < "$PROJECT_DIR/scripts/seed/mysql/01_init.sql"
    
    echo "  Seeding MongoDB..."
    docker compose -f "$COMPOSE_FILE" exec -T test-mongodb mongosh -u testuser -p testpass --authenticationDatabase admin testdb < "$PROJECT_DIR/scripts/seed/mongodb/init.js"
    
    echo "  Seeding SQL Server..."
    docker compose -f "$COMPOSE_FILE" exec -T test-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'TestPass123!' -d testdb -i /docker-entrypoint-initdb.d/init.sql -C
    
    print_success "All databases seeded!"
}

cmd_psql() {
    docker compose -f "$COMPOSE_FILE" exec test-postgres psql -U testuser -d testdb
}

cmd_mysql() {
    docker compose -f "$COMPOSE_FILE" exec test-mysql mysql -u testuser -ptestpass testdb
}

cmd_mongo() {
    docker compose -f "$COMPOSE_FILE" exec test-mongodb mongosh -u testuser -p testpass --authenticationDatabase admin testdb
}

cmd_mssql() {
    docker compose -f "$COMPOSE_FILE" exec test-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'TestPass123!' -d testdb -C
}

cmd_help() {
    echo "Warehouse Development Environment Script"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  start      Start all test database containers"
    echo "  stop       Stop all test database containers"
    echo "  restart    Restart all containers"
    echo "  status     Show status of all containers"
    echo "  logs       Show logs from all containers (or logs <service>)"
    echo "  reset      Stop, remove volumes, and start fresh"
    echo "  seed       Re-run seed scripts (containers must be running)"
    echo "  psql       Connect to test PostgreSQL"
    echo "  mysql      Connect to test MySQL"
    echo "  mongo      Connect to test MongoDB"
    echo "  mssql      Connect to test SQL Server"
    echo "  help       Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 start           # Start all test databases"
    echo "  $0 logs test-mysql # Show MySQL container logs"
    echo "  $0 psql            # Connect to PostgreSQL CLI"
}

# Main
check_docker

case "${1:-help}" in
    start)
        cmd_start
        ;;
    stop)
        cmd_stop
        ;;
    restart)
        cmd_restart
        ;;
    status)
        cmd_status
        ;;
    logs)
        cmd_logs "$2"
        ;;
    reset)
        cmd_reset
        ;;
    seed)
        cmd_seed
        ;;
    psql)
        cmd_psql
        ;;
    mysql)
        cmd_mysql
        ;;
    mongo)
        cmd_mongo
        ;;
    mssql)
        cmd_mssql
        ;;
    help|--help|-h)
        cmd_help
        ;;
    *)
        print_error "Unknown command: $1"
        echo ""
        cmd_help
        exit 1
        ;;
esac
