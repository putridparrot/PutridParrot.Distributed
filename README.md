# PutridParrot.Distributed

Flexible, cache-agnostic distributed patterns library for .NET 10 that works with Redis, Valkey, SQL Server, PostgreSQL, and other distributed systems.

Includes:
- **Distributed Leader Election** - Single leader coordination among multiple candidates

## Architecture

Patterns follow a **pluggable provider model**:
1. **Core Library** (`PutridParrot.Distributed`) - Pattern implementations and interfaces
2. **Backend Providers** - Cache/database-specific implementations PutridParrot.Distributed.*CacheName* (Redis, SQL Server, PostgreSQL)
3. **Console Demo** (`PutridParrot.Distributed.Console`) - Interactive examples and usage patterns

## Projects

### PutridParrot.Distributed

**Distributed Leader Election Features:**
- Redis implementation (SET NX for atomicity, Lua for renewal)
- SQL Server implementation (serializable transactions, renewal tracking)
- PostgreSQL implementation (INSERT...ON CONFLICT coordination)
- 6 interactive examples
- Single-leader coordination, heartbeat renewal, graceful yield, multi-candidate competition

**Distributed Lock Components:**
- `IDistributedCacheProvider` - Interface for lock backends
- `DistributedLock` - Main lock implementation
- `DistributedLockOptions` - Lock configuration
- `DistributedLockFactory` - Lock factory with default options
