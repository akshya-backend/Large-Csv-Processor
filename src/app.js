const cluster = require('cluster');
const os = require('os');
const express = require('express');
const convertRoute = require('./routes/convert');
const { logger } = require('./utils/logger');
const { errorHandler } = require('./utils/errorHandler');

const PORT = 3001;
const numCPUs = os.cpus().length;

if (cluster.isPrimary) {
  logger.info(`Primary ${process.pid} is running`);
  logger.info(`Forking ${numCPUs} workers...`);

  for (let i = 0; i < numCPUs; i++) cluster.fork();

  cluster.on('exit', (worker, code, signal) => {
    logger.warn(`Worker ${worker.process.pid} died. Restarting...`);
    cluster.fork();
  });
} else {
  const app = express();
  app.use(express.json());
  app.use('/api/convert', convertRoute);
  app.use(errorHandler);

  app.listen(PORT, () => {
    logger.info(` Worker ${process.pid} running on http://localhost:${PORT}`);
  
  });

  module.exports = app;
}
