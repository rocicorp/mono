# Bencher Metrics API Endpoint

## Endpoint
`GET /api/metrics/search`

## Query Parameters

- `search` (optional): Search string to filter benchmark names
- `page` (default: 1): Page number for pagination
- `perPage` (default: 100, max: 100): Number of sparklines per page

## Example Request

```bash
# Search for all metrics containing "latency"
curl "http://localhost:3000/api/metrics/search?search=latency&page=1&perPage=50"

# Get all metrics (paginated)
curl "http://localhost:3000/api/metrics/search?page=1&perPage=100"
```

## Response Format

```json
{
  "sparklines": [
    {
      "benchmarkId": "uuid-here",
      "benchmarkName": "test_latency_p99",
      "data": [
        {
          "timestamp": 1699816009000,
          "value": 45.2
        },
        {
          "timestamp": 1699816109000,
          "value": 44.8
        }
      ]
    }
  ],
  "pagination": {
    "page": 1,
    "perPage": 100,
    "total": 250,
    "totalPages": 3
  },
  "config": {
    "project": "your-project",
    "branch": "branch-uuid",
    "testbed": "testbed-uuid",
    "measure": "measure-uuid",
    "lookbackMs": 604800000
  }
}
```

## Configuration

Set the following environment variables in `.env.local`:

```env
BENCHER_API_TOKEN=your_api_token
BENCHER_PROJECT=your_project_slug
BENCHER_BRANCH=branch_uuid
BENCHER_TESTBED=testbed_uuid
BENCHER_MEASURE=measure_uuid
BENCHER_API_URL=https://api.bencher.dev
```

## Notes

- Data lookback period is 1 week (7 days)
- Maximum 100 sparklines per page
- Public Bencher projects don't require an API token
- Private projects require a valid API token with `view` permissions