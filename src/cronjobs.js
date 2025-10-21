import cron from "cron";
import { updateBookings } from "./jobs/aUpdateBookings.js";
import { processTransactions } from "./jobs/bProcessTransactions.js";
import { processRefunds } from "./jobs/cIntiateRefunds.js";
import { processWalletWithdrawals } from "./jobs/dProcessWalletWithdrawals.js";
import { logMessage } from "./logger.js"; // Logger for tracking
import dotenv from "dotenv";
dotenv.config();

// Define cron jobs with specific schedules
const jobs = [
  {
    name: "update-bookings",
    schedule: "15 1 * * *", // Every day at 1:00 AM
    task: updateBookings,
  },
  {
    name: "process-transactions",
    schedule: "30 1 * * *", // Every day at 1:30 AM
    task: processTransactions,
  },
  {
    name: "process-refunds",
    schedule: "0 2 * * *", // Every day at 2:00 AM
    task: processRefunds,
  },
  {
    name: "stripe-wallet-withdrawals",
    schedule: "30 2 * * *", // Every day at 2:30 AM
    task: processWalletWithdrawals,
  },
];

// Initialize and start cron jobs
jobs.forEach(({ name, schedule, task }) => {
  new cron.CronJob(
    schedule,
    async () => {
      logMessage(name, `Started job at ${new Date().toISOString()}`);
      try {
        await task();
        logMessage(
          name,
          `Completed successfully at ${new Date().toISOString()}`
        );
      } catch (error) {
        logMessage(name, `Error: ${error.message}`, true);
      }
    },
    null,
    true
  );

  logMessage("cron-scheduler", `Scheduled job: ${name} -> ${schedule}`);
});
