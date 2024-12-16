# Use a Node.js + Playwright base image
FROM mcr.microsoft.com/playwright:v1.47.2-focal

# Set working directory
WORKDIR /app

# Create a minimal package.json within the container
RUN echo '{ \
  "name": "playwright-tests", \
  "version": "1.0.0", \
  "scripts": { "test": "playwright test" }, \
  "dependencies": { "@playwright/test": "^1.47.2-focal" } \
  }' > package.json

# Install only Playwright and its dependencies
RUN npm install

# Copy your Playwright test scripts (adjust to your actual directory structure)
COPY *.spec.ts .
COPY start.sh .
RUN chmod +x ./start.sh


# Run the Playwright tests
CMD ["./start.sh"]