import { db } from "../db.js";
import { sql as dsql } from "drizzle-orm";
import Stripe from "stripe";
import { logMessage } from "../logger.js";

export async function processTransactions() {
  const stripe = new Stripe(process.env.STRIPE_SECRET.trim());
  const jobName = "process-transactions";
  try {
    logMessage(jobName, "Starting transaction processing...");

    // Fetch bookings with transaction_status = "processing"
    const bookingsQuery = dsql`
      SELECT id, amount, admin, transaction_id, topic, payment_intent_id
      FROM bookings
      WHERE transaction_status = 'processing';
    `;
    const bookings = await db.execute(bookingsQuery);

    if (bookings.rows.length === 0) {
      logMessage(
        jobName,
        "No bookings with 'processing' transaction_status found."
      );
      return;
    }

    // Process each booking individually (not in a single transaction)
    for (const booking of bookings.rows) {
      const { id, amount, admin, transaction_id, topic } = booking;
      logMessage(
        jobName,
        `Processing Booking ID: ${id}, Amount: ${amount}, Admin: ${admin}`
      );

      try {
        // CRITICAL: Verify the payment actually succeeded in Stripe before crediting wallet
        const paymentIntent = await stripe.paymentIntents.retrieve(
          booking.payment_intent_id
        );

        if (paymentIntent.status !== "succeeded") {
          logMessage(
            jobName,
            `Payment verification failed for Booking ID: ${id}. Stripe status: ${paymentIntent.status}`,
            true
          );

          // Mark as failed, don't credit wallet
          await db.execute(
            dsql`UPDATE bookings
                 SET transaction_status = 'payment_verification_failed'
                 WHERE id = ${id}`
          );
          continue;
        }

        logMessage(
          jobName,
          `Payment verified for Booking ID: ${id}. PaymentIntent: ${paymentIntent.id}, Status: ${paymentIntent.status}`
        );

        // Payment verified, now credit the admin's wallet
        await db.transaction(async (trx) => {
          // Fetch the current wallet balance of the admin
          const walletQuery = dsql`
            SELECT id, amount
            FROM wallet
            WHERE email = ${admin} AND currency = 'USD' LIMIT 1;
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

          logMessage(jobName, `New Balance for Admin ${admin}: ${newBalance}`);

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
                   VALUES (${newBalance}, 'USD', 'active', ${admin})`
            );
          }

          // Insert transaction record
          await trx.execute(
            dsql`INSERT INTO transactions (date, transaction_id, type, amount, email, "to", "from", message)
                 VALUES (
                   NOW(),
                   ${`to_wallet_${transaction_id}`},
                   'credit',
                   ${amount},
                   ${admin},
                   'wallet',
                   'system',
                   ${
                     "Credited $" + amount + ` for booking completion: ${topic}`
                   }
                 )`
          );

          // Update booking's transaction_status to "completed"
          await trx.execute(
            dsql`UPDATE bookings
                 SET transaction_status = 'completed'
                 WHERE id = ${id}`
          );
        });

        logMessage(
          jobName,
          `Booking ID: ${id} updated to transaction_status: 'completed'`
        );
      } catch (error) {
        logMessage(
          jobName,
          `Error processing Booking ID: ${id}: ${error.message}`,
          true
        );
        // Don't update status, will retry on next run
      }
    }

    logMessage(jobName, "All transactions processed successfully.");
  } catch (error) {
    logMessage(
      jobName,
      `Error processing transactions: ${error.message}`,
      true
    );
  }
}
