# zbugs-lite-solid

A minimal Zero + Solid.js demo app based on the zbugs project.

## Features

- **Solid.js Reactivity**: Demonstrates Zero's integration with Solid.js reactive system
- **GitHub Login**: OAuth authentication via zbugs API server
- **Real-time Sync**: Live issue updates across clients using Zero
- **Create & Edit**: Add new issues and edit existing ones inline

## Setup

### Prerequisites

1. PostgreSQL running (from zbugs)
2. Node.js 20+

### Installation

```bash
# From repository root
npm install

# Create environment file (or symlink to zbugs .env)
cd apps/zbugs-lite-solid
ln -s ../zbugs/.env .env

# Or copy and configure separately
cp .env.example .env
```

### Running

**Important**: This app requires the zbugs API server (integrated via Vite config).

1. **Start PostgreSQL** (if not already running):

   ```bash
   cd apps/zbugs
   npm run db-up
   npm run db-migrate
   npm run db-seed
   ```

2. **Start zero-cache server**:

   ```bash
   cd apps/zbugs
   npm run zero-cache-dev
   ```

3. **Start zbugs-lite-solid**:
   ```bash
   cd apps/zbugs-lite-solid
   npm run dev
   ```

The app will be available at http://localhost:5173

## Login

Click "Login" in the top-right to authenticate with GitHub. After login, you'll be able to:

- Create new issues
- Edit issue titles
- See your username displayed

Logout is available via the button next to your name.

## Architecture

- **Frontend**: Solid.js with Zero sync
- **Backend**: Shares zbugs API server (Fastify) for mutations/queries
- **Auth**: JWT stored in cookie, GitHub OAuth via `/api/login/github`
- **Database**: PostgreSQL → Zero cache → Client (IndexedDB)

## Development

```bash
npm run dev          # Start dev server
npm run build        # Build for production
npm run check-types  # TypeScript type checking
npm run lint         # Lint with oxlint
npm run format       # Format with prettier
```
