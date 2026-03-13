# ----------------------------------------
# Stage 1: Builder
# ----------------------------------------
FROM node:22-bullseye AS builder

WORKDIR /usr/src/app

# Copy config files
COPY package.json yarn.lock ./

# Install ALL dependencies (including devDeps like esbuild/tsx)
RUN yarn install --frozen-lockfile

# Copy source code
COPY . .

# Generate artifacts (knownMsgs.ts)
RUN npx tsx scripts/gen-known-msgs.ts

# ✅ FIX 1: Build the Main App (with --format=esm)
# This fixes the "import.meta is not available" warning
RUN npx esbuild src/index.ts \
    --bundle \
    --platform=node \
    --packages=external \
    --format=esm \
    --outfile=dist/index.js \
    --target=node22

# ✅ FIX 2: Build the Worker (Separate entry point)
# The worker runs in a separate thread, so it needs its own bundled file
RUN npx esbuild src/decode/txWorker.ts \
    --bundle \
    --platform=node \
    --packages=external \
    --format=esm \
    --outfile=dist/txWorker.js \
    --target=node22

# ✅ FIX 4: Build the Genesis Bootstrap Script
RUN npx esbuild src/scripts/genesis-bootstrap.ts \
    --bundle \
    --platform=node \
    --packages=external \
    --format=esm \
    --outfile=dist/genesis-bootstrap.js \
    --target=node22

# ✅ FIX 3: Trick the runtime
# Your code asks for "./txWorker.ts", but we have compiled it to JS.
# We create a copy named .ts so the runtime finds it immediately without changing source code.
RUN cp dist/txWorker.js dist/txWorker.ts

# ----------------------------------------
# Stage 2: Runner (Production)
# ----------------------------------------
FROM node:22-bullseye-slim AS runner

WORKDIR /usr/src/app

ENV NODE_ENV=production

# Install basic OS libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy package files
COPY package.json yarn.lock ./

# Install ONLY production dependencies
RUN yarn install --frozen-lockfile --production

# Copy the bundled files from Builder stage
COPY --from=builder /usr/src/app/dist ./dist
COPY --from=builder /usr/src/app/protos ./protos
COPY --from=builder /usr/src/app/docker-entrypoint.sh ./

# Permission & Security
RUN chmod +x ./docker-entrypoint.sh && \
    chown -R node:node /usr/src/app

USER node

EXPOSE 3000

ENTRYPOINT ["./docker-entrypoint.sh"]

# Run the bundled file
CMD ["node", "dist/index.js"]
