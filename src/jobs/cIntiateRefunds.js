import { db } from "../db.js";
import { sql as dsql } from "drizzle-orm";
import { logMessage } from "../logger.js";

export async function processRefunds() {
  const jobName = "process-refunds";
  try {
    logMessage(jobName, "Starting refund processing...");

    // Fetch bookings with transaction_status = "refund"
    const refundQuery = dsql`
      SELECT id, amount, joinee, transaction_id, topic
      FROM bookings 
      WHERE transaction_status = 'processRefund' and payment=true;
    `;
    const refunds = await db.execute(refundQuery);

    if (refunds.rows.length === 0) {
      logMessage(
        jobName,
        "No bookings with 'refund' transaction_status found."
      );
      return;
    }

    // Process each refund in a transaction
    await db.transaction(async (trx) => {
      for (const refund of refunds.rows) {
        const { id, amount, joinee, transaction_id, topic } = refund;
        logMessage(
          jobName,
          `Processing Refund for Booking ID: ${id}, Amount: ${amount}, Joinee: ${joinee}`
        );

        // Fetch the current wallet balance of the joinee
        const walletQuery = dsql`
          SELECT id, amount 
          FROM wallet 
          WHERE email = ${joinee} AND currency = 'USD' LIMIT 1;
        `;
        const walletData = await trx.execute(walletQuery);

        let walletId, newBalance;
        if (walletData.rows.length > 0) {
          walletId = walletData.rows[0].id;
          newBalance =
            parseFloat(walletData.rows[0].amount) + parseFloat(amount);
        } else {
          newBalance = amount; // If no wallet exists, create a new one
        }

        logMessage(jobName, `New Balance for Joinee ${joinee}: ${newBalance}`);

        // Update or Insert into wallet
        if (walletId) {
          await trx.execute(
            dsql`UPDATE wallet 
                 SET amount = ${newBalance} 
                 WHERE id = ${walletId}`
          );
        } else {
          await trx.execute(
            dsql`INSERT INTO wallet (amount, currency, status, email) 
                 VALUES (${newBalance}, 'USD', 'active', ${joinee})`
          );
        }

        // Insert transaction record
        await trx.execute(
          dsql`INSERT INTO transactions (date, transaction_id, type, amount, email, "to", "from", message) 
               VALUES (
                 NOW(), 
                 ${`refund_${transaction_id}`},
                 'credit', 
                 ${amount}, 
                 ${joinee}, 
                 'wallet', 
                 'system', 
                 ${
                   "Refunded $" + amount + ` for booking cancellation: ${topic}`
                 }
               )`
        );

        // Update booking's transaction_status to "refunded"
        await trx.execute(
          dsql`UPDATE bookings 
               SET transaction_status = 'refunded' 
               WHERE id = ${id}`
        );

        logMessage(
          jobName,
          `Booking ID: ${id} updated to transaction_status: 'refunded'`
        );
      }
    });

    logMessage(jobName, "All refunds processed successfully.");
  } catch (error) {
    logMessage(jobName, `Error processing refunds: ${error.message}`, true);
  }
}
