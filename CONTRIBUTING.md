# Contributing to RPresto

We would love to have your help in making RPresto better. We actively
welcome your pull requests.
1. Fork the repo and create your branch from `master`.
2. If you've added code that should be tested, add tests
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes.
5. If you haven't already, complete the Contributor License Agreement ("CLA").

## Our Development Process

We develop our code using the [devtools](https://github.com/hadley/devtools)
package. You can use the 'devtools::check' function to build and verify that
your changes are working as intended.

## Testing

RPresto uses a comprehensive testing strategy that includes both unit tests and integration tests against live Presto and Trino database servers. This ensures that your code works correctly with real database engines.

### Prerequisites

Before running tests, ensure you have:
- **Docker** installed and running
- **Docker Compose** available
- **R** with required packages (devtools, testthat, etc.)

### Setting Up Test Databases

The project includes Docker Compose configuration to run both Presto and Trino servers locally:

```bash
# Start both Presto and Trino servers
docker-compose up -d

# Verify servers are running
curl http://localhost:8080/v1/info  # Presto
curl http://localhost:8090/v1/info  # Trino
```

**Server Details:**
- **Presto**: Available on `http://localhost:8080`
- **Trino**: Available on `http://localhost:8090`
- **Memory Connector**: Both use in-memory storage for fast testing

### Running Tests

#### Option 1: Test Against Presto (Default)
```r
# Set environment variable for Presto testing
Sys.setenv(PRESTO_TYPE = "Presto")

# Run all tests
devtools::test()

# Or run specific test files
devtools::test_file("tests/testthat/test-dbConnect.R")
```

#### Option 2: Test Against Trino
```r
# Set environment variable for Trino testing
Sys.setenv(PRESTO_TYPE = "Trino")

# Run all tests
devtools::test()

# Or run specific test files
devtools::test_file("tests/testthat/test-dbConnect.R")
```

#### Option 3: Test Both Engines
```r
# Test Presto
Sys.setenv(PRESTO_TYPE = "Presto")
devtools::test()

# Test Trino
Sys.setenv(PRESTO_TYPE = "Trino")
devtools::test()
```

### Test Environment Variables

The test suite uses these environment variables to control behavior:

- **`PRESTO_TYPE`**: Controls which database engine to test against
  - `"Presto"` (default): Tests against Presto server
  - `"Trino"`: Tests against Trino server
- **`PRESTO_VERSION`**: Docker image version for Presto (defaults to "latest")
- **`TRINO_VERSION`**: Docker image version for Trino (defaults to "latest")

### Test Categories

The test suite covers:

1. **Unit Tests**: Fast tests that don't require database connections
2. **Integration Tests**: Tests that require live database connections
3. **Engine-Specific Tests**: Tests that validate Presto vs Trino compatibility
4. **Performance Tests**: Tests that measure query execution time
5. **Error Handling**: Tests that validate proper error responses

### Writing New Tests

When adding new functionality, follow these guidelines:

1. **Test Both Engines**: Ensure your tests work with both Presto and Trino
2. **Use Live Connections**: Prefer integration tests over mocked tests
3. **Test Error Cases**: Include tests for invalid inputs and error conditions
4. **Performance Considerations**: Test with realistic data sizes when possible

Example test structure:
```r
test_that("new_functionality works with both engines", {
  # Test with Presto
  Sys.setenv(PRESTO_TYPE = "Presto")
  con <- setup_live_connection()
  # ... test logic ...
  
  # Test with Trino
  Sys.setenv(PRESTO_TYPE = "Trino")
  con <- setup_live_connection()
  # ... test logic ...
})
```

### Troubleshooting

#### Common Issues

1. **Docker containers not starting**:
   ```bash
   # Check container status
   docker ps
   
   # View logs
   docker logs presto
   docker logs trino
   
   # Restart services
   docker-compose down
   docker-compose up -d
   ```

2. **Port conflicts**:
   - Ensure ports 8080 and 8090 are available
   - Stop any existing Presto/Trino services

3. **Memory issues**:
   - Presto uses 2GB, Trino uses 16GB by default
   - Adjust JVM settings in config files if needed

4. **Test failures**:
   - Verify both servers are running and healthy
   - Check that `PRESTO_TYPE` environment variable is set correctly
   - Ensure test database connections are working

#### Getting Help

- Check the test logs for specific error messages
- Verify Docker containers are running and healthy
- Ensure environment variables are set correctly
- Review the test setup in `tests/testthat/setup.R`

### Continuous Integration

The project automatically runs tests in GitHub Actions with:
- Multiple R versions
- Both Presto and Trino engines
- Automated Docker setup and teardown
- Test coverage reporting

Your local testing should match the CI environment for best results.

## Contributor License Agreement ("CLA")
In order to accept your pull request, we need you to submit a CLA. You only need
to do this once to work on any of Facebook's open source projects.

Complete your CLA here: <https://code.facebook.com/cla>

## Issues
We use GitHub issues to track public bugs. Please ensure your description is
clear and has sufficient instructions to be able to reproduce the issue.

Facebook has a [bounty program](https://www.facebook.com/whitehat/) for the safe
disclosure of security bugs. In those cases, please go through the process
outlined on that page and do not file a public issue.

## License
By contributing to RPresto, you agree that your contributions will be licensed
under its BSD license.
