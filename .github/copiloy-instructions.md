# Binge Works ETL Pipeline - Copilot Instructions

## Project Overview

This project implements an ETL (Extract, Transform, Load) pipeline using Dagster to process movie and TV show data from The Movie Database (TMDB) API. The pipeline extracts data about popular shows, movies, and people, then transforms and loads it into a PostgreSQL database hosted on Azure.

## Architecture

The project follows a modern ELT architecture:

- **Extract**: Data is pulled from TMDB API
- **Load**: Raw data is loaded into staging tables
- **Transform**: Transformations are applied and data is organized into production tables

## Key Components

#### Library standards

- **Loading Dagster**: When loading dagster always load it as:
  `import dagster as dg`
  So that you can call the dagster classes and methods as:
  `@dg.asset`
  Or
  `return dg.Output()`

### Resources

- **TMDBResource**: Handles API calls to TMDB
- **PostgreSQLResource**: Manages database connections and operations

### Asset Groups

1. **Popular Shows**: TV show data extraction and processing
2. **Popular People**: Person data extraction and processing
3. **Popular Movies**: Movie data extraction and processing
4. **Dimension Tables**: Dimensional modeling for people and shows
5. **Bridge Tables**: Many-to-many relationships between entities

### Data Flow

The data moves through these layers:

- **Source (Bronze)**: Raw data from API
- **Staging (Silver)**: Intermediate tables with minimal transformations
- **Production (Gold)**: Final tables with business rules applied

## Development Guidelines

### Adding a New Data Pipeline

1. **Create a new folder** in the `assets` directory (e.g., `new_asset_type/`)
2. **Add an `__init__.py`** file that will export the asset functions
3. **Create the main asset file** (e.g., `new_asset_type.py`) with extraction, transformation, and loading assets
4. **Add constants** for the asset group in `constants.py`
5. **Update `definitions.py`** to load assets using `load_assets_from_package_module`
6. **Create jobs and schedules** if needed

Example for `definitions.py`:

```python
# Import the new asset module
from .assets import new_asset_type

# Import constants
from .constants import NEW_ASSET_TYPE

# Load assets
new_asset_type_assets = load_assets_from_package_module(
    new_asset_type,
    group_name=NEW_ASSET_TYPE
)

# Add to asset_defs list
asset_defs = [
    *existing_assets,
    *new_asset_type_assets
]
```

### Working with Assets

Assets follow this pattern:

```python
@dg.asset(
    name="asset_name",
    deps=[dependencies],
    required_resource_keys={"resource_dependencies"},
    kinds={"asset_types"},
    tags={'layer':'categorization'}
)
def asset_function(context, ...):
    # Implementation
    return Output(value, metadata={...})
```

### Type 2 Slowly Changing Dimensions (SCD)

For historical tracking, we implement Type 2 SCD pattern with:

- **Current records**: `is_current = true`
- **Historical records**: Record previous values with `effective_date` and `expiration_date`
- **Surrogate keys**: Use surrogate keys (e.g., `id_sk`) for primary keys

### SQL Best Practices

1. **Use staging tables** for initial data loading
2. **Implement UPSERT pattern** for merging data:
   ```sql
   INSERT INTO production_table
   SELECT * FROM staging_table
   ON CONFLICT (key)
   DO UPDATE SET col1 = EXCLUDED.col1
   WHERE table.col1 IS DISTINCT FROM EXCLUDED.col1
   ```
3. **Implement efficient batch operations** using `execute_batch`
4. **Track metadata** about operations (counts, duration, etc.)

## Project Structure

```
binge_works/
├── assets/
│   ├── popular_shows/
│   │   ├── __init__.py        # Exports asset functions
│   │   └── popular_shows.py   # TV shows pipeline
│   ├── popular_people/
│   │   ├── __init__.py        # Exports asset functions
│   │   └── popular_people.py  # People pipeline
│   ├── popular_movies/
│   │   ├── __init__.py        # Exports asset functions
│   │   └── popular_movies.py  # Movies pipeline
│   ├── dim_popular_people/
│   │   ├── __init__.py        # Exports asset functions
│   │   └── dim_popular_people.py # Dimension tables
│   ├── dim_popular_people_history/
│   │   ├── __init__.py        # Exports asset functions
│   │   └── dim_popular_people_history.py # History tables
│   └── bridge_person_project/
│       ├── __init__.py        # Exports asset functions
│       └── bridge_person_project.py # Bridge tables
├── resources/
│   ├── TMDBResource.py        # TMDB API client
│   └── PostgreSQLResource.py  # Database connector
├── jobs/
│   └── jobs.py                # Job definitions
├── schedules/
│   └── schedules.py           # Schedule definitions
├── sensors/
│   └── people_sensors.py      # Asset sensors
├── checks/                    # Asset checks
├── constants.py               # Configuration constants
└── definitions.py             # Dagster definitions
```

## Database Schema

### Staging Tables

- `staging.shows`
- `staging.popular_people`
- `staging.popular_movies`

### Production Tables

- `tmdb_data.dim_popular_shows`
- `tmdb_data.dim_shows_history`
- `tmdb_data.dim_popular_people`
- `tmdb_data.dim_people_history`
- `tmdb_data.popular_movies`
- `tmdb_data.bridge_person_project`

## Common Operations

### Running the Pipeline

```bash
dagster dev               # Start development server
dagster asset materialize # Run specific assets
```

### Debugging Tips

1. Use context.log to add logging:

   ```python
   context.log.info(f"Processing {len(data)} records")
   ```

2. Add metadata to asset outputs:

   ```python
   return Output(
       value=result,
       metadata={
           "execution_time": MetadataValue.float(duration),
           "record_count": MetadataValue.int(count),
       }
   )
   ```

3. Check database directly:
   ```sql
   SELECT COUNT(*) FROM staging.shows;
   ```

## Performance Considerations

1. Use batch operations for database inserts/updates
2. Implement pagination for API calls
3. Use efficient SQL merging patterns
4. Add appropriate indexes to database tables
5. Track execution time in asset metadata

## Security and Configuration

1. Store sensitive configuration in environment variables
2. Use resource definitions for APIs and databases
3. Never commit API keys or connection strings to version control

## Extending the Project

### Adding New Data Sources

1. Create new resource class for API access
2. Create new assets for extraction/transformation
3. Update definitions.py

### Adding Data Quality Checks

1. Create check functions in the checks directory
2. Load them in definitions.py
3. Associate with relevant assets

## Troubleshooting

### Common Issues

1. **API Rate Limiting**: TMDB may rate limit requests - implement backoff and retry
2. **Database Connectivity**: Check connection strings and network access
3. **Schema Evolution**: When API changes, update database schemas accordingly

## Further Reading

- [Dagster Documentation](https://docs.dagster.io/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [TMDB API Documentation](https://developers.themoviedb.org/3)
