import { db } from "../db.js";
import { sql as dsql } from "drizzle-orm";
import Stripe from "stripe";
import { logMessage } from "../logger.js";

export async function processRefunds() {
  const stripe = new Stripe(process.env.STRIPE_SECRET.trim());
  const jobName = "process-refunds";
  try {
    logMessage(jobName, "Starting refund processing...");

    // Fetch bookings with transaction_status = "processRefund" or "refunding"
    // "refunding" captures cases where Stripe succeeded but DB update failed
    const refundQuery = dsql`
      SELECT id, amount, joinee, transaction_id, topic, payment_intent_id
      FROM bookings
      WHERE transaction_status IN ('processRefund', 'refunding') and payment=true;
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
        // Step 1: Mark as "refunding" to track that we're attempting this refund
        await db.execute(
          dsql`UPDATE bookings
               SET transaction_status = 'refunding'
               WHERE id = ${id}`
        );

        logMessage(jobName, `Booking ID: ${id} marked as 'refunding'`);

        // Step 2: Check if refund already exists in Stripe (idempotency check)
        let stripeRefund;
        try {
          const existingRefunds = await stripe.refunds.list({
            payment_intent: refund.payment_intent_id,
            limit: 10,
          });

          // Check if we already created a refund for this payment
          stripeRefund = existingRefunds.data.find(
            (r) => r.status === "succeeded" || r.status === "pending"
          );

          if (stripeRefund) {
            logMessage(
              jobName,
              `Existing Stripe refund found for Booking ID: ${id}, Refund ID: ${stripeRefund.id}`
            );
          }
        } catch (listError) {
          logMessage(
            jobName,
            `Could not check existing refunds for Booking ID: ${id}: ${listError.message}`
          );
        }

        // Step 3: Create Stripe refund if it doesn't exist
        if (!stripeRefund) {
          stripeRefund = await stripe.refunds.create({
            payment_intent: refund.payment_intent_id,
            amount: Math.round(parseFloat(amount) * 100), // Convert to cents
          });

          logMessage(
            jobName,
            `Stripe refund created for Booking ID: ${id}, Refund ID: ${stripeRefund.id}`
          );
        }

        // Step 4: If Stripe refund is successful, update database in a transaction
        if (stripeRefund.id && stripeRefund.status) {
          await db.transaction(async (trx) => {
            // Check if transaction record already exists (prevent duplicates)
            const existingTxn = await trx.execute(
              dsql`SELECT id FROM transactions WHERE transaction_id = ${stripeRefund.id} LIMIT 1`
            );

            if (existingTxn.rows.length === 0) {
              // Insert transaction record only if it doesn't exist
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
            }

            // Check if notification already exists (prevent duplicates)
            const existingNotif = await trx.execute(
              dsql`SELECT id FROM notifications WHERE userid = ${profileId} AND message = ${
                "You have been refunded $" +
                amount +
                ` for booking cancellation: ${topic}`
              } LIMIT 1`
            );

            if (existingNotif.rows.length === 0) {
              // Insert success notification only if it doesn't exist
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
            }

            // Update booking's transaction_status to "refunded"
            await trx.execute(
              dsql`UPDATE bookings
                   SET transaction_status = 'refunded', status='passed'
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
