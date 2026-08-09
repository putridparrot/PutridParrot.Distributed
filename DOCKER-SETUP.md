# Docker Setup for PutridParrot.Distributed Demo

This docker-compose file provides Redis, SQL Server, and PostgreSQL services for running the distributed patterns demos.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) or Docker Engine with Docker Compose

## Quick Start

### Start all services

```bash
docker-compose up -d
```

### Start specific services

```bash
# Start only Redis
docker-compose up -d redis

# Start only SQL Server
docker-compose up -d sqlserver

# Start only PostgreSQL
docker-compose up -d postgresql
```

### Check service status

```bash
docker-compose ps
```

### View logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f redis
docker-compose logs -f sqlserver
docker-compose logs -f postgresql
```

### Stop services

```bash
docker-compose down
```

### Stop services and remove volumes (data will be lost)

```bash
docker-compose down -v
```

## Service Details

### Redis
- **Port**: 6379
- **Connection String**: `localhost:6379`
- **Data persistence**: Enabled with AOF (Append Only File)

### SQL Server
- **Port**: 1433
- **Connection String**: `Server=localhost,1433;Database=distributed;User Id=sa;Password=YourStrong@Password123;TrustServerCertificate=True;`
- **Username**: `sa`
- **Password**: `YourStrong@Password123`
- **Edition**: Developer (free for development/testing)

### PostgreSQL
- **Port**: 5432
- **Connection String**: `Host=localhost;Port=5432;Database=distributed;Username=postgres;Password=YourStrong@Password123;`
- **Username**: `postgres`
- **Password**: `YourStrong@Password123`
- **Database**: `distributed` (automatically created)

## Configuration

The demo application reads connection strings from `appsettings.json`. You can override these by:

1. Editing `appsettings.Development.json`
2. Setting environment variables:
   - `ConnectionStrings__Redis`
   - `ConnectionStrings__SqlServer`
   - `ConnectionStrings__PostgreSQL`

## Security Note

⚠️ **The passwords in this setup are for development/demo purposes only.** 

For production environments:
- Use strong, unique passwords
- Store credentials in a secure vault (Azure Key Vault, HashiCorp Vault, etc.)
- Use managed identities where possible
- Enable TLS/SSL for all connections

## Troubleshooting

### Services not starting

Check the logs:
```bash
docker-compose logs
```

### Port already in use

If a port is already in use, you can modify the port mappings in `docker-compose.yml`. For example, to change Redis to port 6380:

```yaml
redis:
  ports:
	- "6380:6379"  # Changed from 6379:6379
```

Don't forget to update the connection string in `appsettings.json` accordingly.

### SQL Server won't start on Mac M1/M2

SQL Server requires Rosetta 2 on Apple Silicon Macs:
```bash
softwareupdate --install-rosetta
```

Alternatively, use Azure SQL Edge which has ARM64 support:
```yaml
sqlserver:
  image: mcr.microsoft.com/azure-sql-edge:latest
```

## Accessing Services

### Redis CLI

```bash
docker exec -it distributed-redis redis-cli
```

### SQL Server Management

```bash
# Using sqlcmd
docker exec -it distributed-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P YourStrong@Password123 -C

# Or connect with Azure Data Studio, SQL Server Management Studio, or any SQL client
```

### PostgreSQL psql

```bash
docker exec -it distributed-postgresql psql -U postgres -d distributed
```

## Data Persistence

All services use Docker volumes for data persistence:
- `redis-data`: Redis data
- `sqlserver-data`: SQL Server data
- `postgresql-data`: PostgreSQL data

Data persists even after stopping containers. Use `docker-compose down -v` to remove volumes.
