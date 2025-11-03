import { db } from "../db.js";
import { sql as dsql } from "drizzle-orm";
import Stripe from "stripe";
import { logMessage } from "../logger.js";

export async function processRefunds() {
  const stripe = new Stripe(process.env.STRIPE_SECRET.trim());
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

    // Process each refund individually (not in a single transaction)
    for (const refund of refunds.rows) {
      const { id, amount, joinee, transaction_id, topic } = refund;
      logMessage(
        jobName,
        `Processing Refund for Booking ID: ${id}, Amount: ${amount}, Joinee: ${joinee}`
      );

      // Fetch the profile id from the profile table where email is joinee
      const profileQuery = dsql`
        SELECT id
        FROM profile
        WHERE email = ${joinee} LIMIT 1;
      `;
      const profileData = await db.execute(profileQuery);
      let profileId;
      if (profileData.rows.length > 0) {
        profileId = profileData.rows[0].id;
      }

      try {
        // Create Stripe refund - this will refund to the original payment method
        const stripeRefund = await stripe.refunds.create({
          payment_intent: transaction_id,
          amount: Math.round(parseFloat(amount) * 100), // Convert to cents
        });

        logMessage(
          jobName,
          `Stripe refund successful for Booking ID: ${id}, Refund ID: ${stripeRefund.id}`
        );

        // If Stripe refund is successful, update database in a transaction
        if (stripeRefund.id && stripeRefund.status) {
          await db.transaction(async (trx) => {
            // Insert transaction record
            await trx.execute(
              dsql`INSERT INTO transactions (date, transaction_id, type, amount, email, "to", "from", message)
                   VALUES (
                     NOW(),
                     ${stripeRefund.id},
                     'credit',
                     ${amount},
                     ${joinee},
                     ${joinee},
                     'system',
                     ${
                       "Refunded $" +
                       amount +
                       ` for booking cancellation: ${topic}`
                     }
                   )`
            );

            // Insert success notification
            await trx.execute(
              dsql`INSERT INTO notifications (userid, generationid, url, message, type)
                   VALUES (
                     ${profileId},
                     'Study Send Inc',
                     '/',
                     ${
                       "You have been refunded $" +
                       amount +
                       ` for booking cancellation: ${topic}`
                     },
                     'refund'
                   )`
            );

            // Update booking's transaction_status to "refunded"
            await trx.execute(
              dsql`UPDATE bookings
                   SET transaction_status = 'refunded'
                   WHERE id = ${id}`
            );
          });

          logMessage(
            jobName,
            `Booking ID: ${id} updated to transaction_status: 'refunded'`
          );
        }
      } catch (error) {
        // If Stripe refund fails, send failure notification and mark as completed
        logMessage(
          jobName,
          `Stripe refund failed for Booking ID: ${id}: ${error.message}`,
          true
        );

        await db.transaction(async (trx) => {
          // Insert failure notification
          await trx.execute(
            dsql`INSERT INTO notifications (userid, generationid, url, message, type)
                 VALUES (
                   ${profileId},
                   'Study Send Inc',
                   '/',
                   ${
                     "Refund failed for booking: " +
                     topic +
                     `. Please contact support. Error: ${error.message}`
                   },
                   'refund'
                 )`
          );

          // Update booking's transaction_status to "completed"
          await trx.execute(
            dsql`UPDATE bookings
                 SET transaction_status = 'refunded'
                 WHERE id = ${id}`
          );
        });

        logMessage(
          jobName,
          `Booking ID: ${id} marked as 'completed' due to refund failure`
        );
      }
    }

    logMessage(jobName, "All refunds processed successfully.");
  } catch (error) {
    logMessage(jobName, `Error processing refunds: ${error.message}`, true);
  }
}
