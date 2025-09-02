# EXISTS Query Optimization Project

## Initial Goal

The project aims to address performance issues with `whereExists` queries in the incremental view maintenance (IVM) engine. The current implementation has two main components:

1. **Join Node** - Creates the relationship between tables
2. **Exists Filter Operator** - Filters results based on existence

### The Core Problem: Join Order

The key issue is determining the optimal join order - specifically, which table should be the outer loop (first in the pipeline). Ideally, the most constrained or smallest table should be processed first for better performance.

### Current Challenge

The existing join structure creates a tree output, which is problematic when re-ordering joins because:

- Changing join order changes the output structure
- Tree structure makes it difficult to optimize join ordering dynamically

