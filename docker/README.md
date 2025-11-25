# 🐳 CameoDB Docker Configuration

This folder contains Docker configuration files for running CameoDB in a distributed setup.

## Quick Start

```bash
# From the docker directory
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f

# Stop all containers
docker-compose down
```

## What's Included

- **docker-compose.yml**: 3-node distributed CameoDB cluster with nginx load balancer
- **Port Configuration**: External ports 9481-9483 for direct node access, port 80 for load-balanced access
- **Data Persistence**: Volumes for each node's data storage
- **Health Checks**: Container health monitoring enabled

## Access Points

- **Node 1 (Direct)**: http://localhost:9481
- **Node 2 (Direct)**: http://localhost:9482  
- **Node 3 (Direct)**: http://localhost:9483
- **Load Balanced**: http://localhost

## Requirements

- Docker Desktop for macOS
- 4GB+ RAM recommended
- Ports 80, 9481, 9482, 9483, 9581, 9582, 9583 available

For detailed documentation, see the main [README.md](../README.md) file.
