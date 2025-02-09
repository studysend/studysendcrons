import { updateBookings } from "./aUpdateBookings.js";
import { processTransactions } from "./bProcessTransactions.js";
import { processRefunds } from "./cIntiateRefunds.js";
import { processWalletWithdrawals } from "./dProcessWalletWithdrawals.js";
import { logMessage } from "../logger.js";

// Define jobs in sequence with 1-minute gap
const jobs = [
  { name: "update-bookings", task: updateBookings },
  { name: "process-transactions", task: processTransactions },
  { name: "process-refunds", task: processRefunds },
  { name: "wallet-withdrawals", task: processWalletWithdrawals },
];

async function runJobsSequentially() {
  for (const { name, task } of jobs) {
    logMessage("job-runner", `Starting job: ${name}`);
    try {
      await task();
      logMessage("job-runner", `Job ${name} completed successfully`);
    } catch (error) {
      logMessage("job-runner", `Error in job ${name}: ${error.message}`, true);
    }

    logMessage("job-runner", `Waiting 1 minute before the next job...`);
    await new Promise((resolve) => setTimeout(resolve, 60 * 1000)); // 1-minute delay
  }

  logMessage("job-runner", "All jobs completed.");
}

// Run the function
runJobsSequentially();
