import { updateBookings } from "./aUpdateBookings.js";
import { processRefunds } from "./cIntiateRefunds.js";
import { processWalletWithdrawals } from "./dProcessWalletWithdrawals.js";
import { processTransactions } from "./bProcessTransactions.js";
// step 1 to mark the data
// updateBookings()
//   .then(() => console.log("Update completed."))
//   .catch((err) => console.error("Error:", err));

// step 2 to process refunds
// processRefunds()
//   .then(() => console.log("Refund processing completed."))
//   .catch((err) => console.error("Error:", err));

// processTransactions()
//   .then(() => console.log("Transaction processing completed."))
//   .catch((err) => console.error("Error:", err));

// step 5: process wallet withdrawals
processWalletWithdrawals()
  .then(() => console.log("Wallet withdrawal processing completed."))
  .catch((err) => console.error("Error:", err));
