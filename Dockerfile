# Stage 1: Build
FROM node:18-alpine AS builder

# Set working directory inside the container
WORKDIR /app

# Install Yarn globally
RUN corepack enable && corepack prepare yarn@stable --activate

# Copy package.json and yarn.lock first to leverage Docker cache
COPY package.json yarn.lock ./

# Install dependencies (production false so it installs devDeps for build)
RUN yarn

# Copy the rest of your application code
COPY . .

# Build TypeScript files to JavaScript (into /dist)
RUN npx tsc

# Stage 2: Production environment
FROM node:18-alpine AS production

WORKDIR /app

# Install Yarn globally in production container
RUN corepack enable && corepack prepare yarn@stable --activate

# Copy only the necessary files from the builder stage
COPY --from=builder /app/package.json /app/yarn.lock ./
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules

# Set environment variables (optional)
ENV NODE_ENV=production
ENV PORT=3000

# Expose the port your app runs on
EXPOSE 3000

# Run the app!
CMD ["node", "dist/app.js"]
