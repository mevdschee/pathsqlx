#!/bin/bash
# Setup script for pathsqlx test databases
# Run with: sudo ./scripts/setup-databases.sh

set -e

DB_USER="pathql"
DB_PASS="pathql"
DB_NAME="pathql"

echo "=== Setting up MariaDB/MySQL ==="
mysql -e "CREATE DATABASE IF NOT EXISTS ${DB_NAME};"
mysql -e "CREATE USER IF NOT EXISTS '${DB_USER}'@'localhost' IDENTIFIED BY '${DB_PASS}';"
mysql -e "GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${DB_USER}'@'localhost';"
mysql -e "FLUSH PRIVILEGES;"
echo "MariaDB setup complete."

echo ""
echo "=== Setting up PostgreSQL ==="
sudo -u postgres psql -c "CREATE USER ${DB_USER} WITH PASSWORD '${DB_PASS}';" 2>/dev/null || \
    sudo -u postgres psql -c "ALTER USER ${DB_USER} WITH PASSWORD '${DB_PASS}';"
sudo -u postgres psql -c "CREATE DATABASE ${DB_NAME} OWNER ${DB_USER};" 2>/dev/null || \
    echo "PostgreSQL database already exists."
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};"
echo "PostgreSQL setup complete."

echo ""
echo "=== Done ==="
echo "You can now run: go test -v"
