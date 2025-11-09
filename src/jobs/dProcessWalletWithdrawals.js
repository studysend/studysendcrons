import { db } from "../db.js";
import { sql as dsql } from "drizzle-orm";
import Stripe from "stripe";
import { logMessage } from "../logger.js";

export async function processWalletWithdrawals() {
  const stripe = new Stripe(process.env.STRIPE_SECRET.trim());
  const jobName = "wallet-withdrawals";

  try {
    logMessage(jobName, "Starting wallet withdrawal process...");

    // Fetch eligible wallets with row-level locking to prevent concurrent processing
    // Include 'withdrawing' status to handle incomplete transfers
    const walletRows = await db.execute(
      dsql`
        SELECT id, email, amount, withdrawal_status, stripe_transfer_id 
        FROM wallet 
        WHERE amount >= 10 
          AND status = 'active'
          AND (withdrawal_status IS NULL OR withdrawal_status = 'withdrawing')
        FOR UPDATE SKIP LOCKED
      `
    );

    if (walletRows.rows.length === 0) {
      logMessage(jobName, "No eligible wallets found for transfer.");
      return;
    }

    for (const wallet of walletRows.rows) {
      await processWalletWithdrawal(stripe, jobName, wallet);
    }

    logMessage(jobName, "All wallet withdrawals processed ✅");
  } catch (error) {
    logMessage(jobName, `Critical error: ${error.message}`, true);
  }
}

async function processWalletWithdrawal(stripe, jobName, wallet) {
  const {
    id: walletId,
    email: userEmail,
    amount,
    withdrawal_status,
    stripe_transfer_id,
  } = wallet;
  const walletBalance = parseFloat(amount);

  logMessage(jobName, `Processing ${userEmail} with $${walletBalance}`);

  try {
    // Fetch Stripe account ID and profile ID
    const profileData = await db.execute(
      dsql`SELECT id, stripe_account_id FROM profile WHERE email = ${userEmail}`
    );

    if (
      profileData.rows.length === 0 ||
      !profileData.rows[0].stripe_account_id
    ) {
      logMessage(jobName, `No Stripe account for ${userEmail}`, true);
      return;
    }

    const profileId = profileData.rows[0].id;
    const stripeAccountId = profileData.rows[0].stripe_account_id;

    // Generate a unique idempotency key based on wallet ID and amount
    const idempotencyKey = `wallet_withdrawal_${walletId}_${walletBalance.toFixed(
      2
    )}`;
    let transfer = null;

    // Case 1: Check if we already have a stripe_transfer_id stored (recovery case)
    if (stripe_transfer_id) {
      logMessage(
        jobName,
        `Found existing transfer ID for ${userEmail}: ${stripe_transfer_id}`
      );
      try {
        transfer = await stripe.transfers.retrieve(stripe_transfer_id);

        if (transfer.status === "failed" || transfer.status === "canceled") {
          logMessage(
            jobName,
            `Previous transfer ${stripe_transfer_id} failed/canceled, will retry`
          );
          transfer = null; // Will create a new transfer
        } else {
          logMessage(
            jobName,
            `Completing DB operations for existing transfer ${stripe_transfer_id}`
          );
        }
      } catch (retrieveError) {
        logMessage(
          jobName,
          `Could not retrieve transfer ${stripe_transfer_id}: ${retrieveError.message}`
        );
        transfer = null; // Will create a new transfer
      }
    }

    // Case 2: If no stored transfer_id, check if transfer exists in Stripe
    if (!transfer) {
      try {
        const recentTransfers = await stripe.transfers.list({
          destination: stripeAccountId,
          limit: 50,
        });

        // Look for a matching successful transfer with same amount
        transfer = recentTransfers.data.find(
          (t) =>
            t.amount === Math.round(walletBalance * 100) &&
            (t.status === "paid" || t.status === "pending") &&
            // Only consider transfers created in the last 7 days to avoid false matches
            t.created > Date.now() / 1000 - 7 * 24 * 60 * 60
        );

        if (transfer) {
          logMessage(
            jobName,
            `Found existing Stripe transfer for ${userEmail}: ${transfer.id}`
          );
        }
      } catch (listError) {
        logMessage(
          jobName,
          `Could not check existing transfers: ${listError.message}`
        );
      }
    }

    // Case 3: Create new Stripe transfer if needed
    if (!transfer) {
      // Mark wallet as withdrawing BEFORE creating Stripe transfer
      await db.execute(
        dsql`
          UPDATE wallet 
          SET withdrawal_status = 'withdrawing' 
          WHERE id = ${walletId} AND withdrawal_status IS NULL
        `
      );

      logMessage(jobName, `Creating new Stripe transfer for ${userEmail}`);

      transfer = await stripe.transfers.create(
        {
          amount: Math.round(walletBalance * 100),
          currency: "usd",
          destination: stripeAccountId,
          description: `Wallet withdrawal for ${userEmail}`,
          metadata: {
            wallet_id: walletId.toString(),
            user_email: userEmail,
          },
        },
        {
          idempotencyKey: idempotencyKey,
        }
      );

      logMessage(
        jobName,
        `Stripe transfer created: ${transfer.id} with status: ${transfer.status}`
      );

      // Store the transfer ID immediately after creation
      await db.execute(
        dsql`
          UPDATE wallet 
          SET stripe_transfer_id = ${transfer.id} 
          WHERE id = ${walletId}
        `
      );
    }

    // Verify transfer is successful before proceeding
    if (!transfer || !transfer.id) {
      throw new Error("Transfer creation failed - no transfer ID returned");
    }

    if (transfer.status === "failed" || transfer.status === "canceled") {
      throw new Error(`Transfer ${transfer.id} has status: ${transfer.status}`);
    }

    // Now complete the database transaction atomically
    await db.transaction(async (trx) => {
      const transactionId = `to_stripe_${transfer.id}`;

      // Check if transaction record already exists
      const existingTxn = await trx.execute(
        dsql`SELECT id FROM transactions WHERE transaction_id = ${transactionId} LIMIT 1`
      );

      if (existingTxn.rows.length === 0) {
        await trx.execute(
          dsql`
            INSERT INTO transactions (type, amount, email, "to", "from", message, transaction_id)
            VALUES (
              ${"credit"},
              ${walletBalance},
              ${userEmail},
              ${stripeAccountId},
              ${"company_account"},
              ${`Transferred $${walletBalance} to Stripe account`},
              ${transactionId}
            )
          `
        );
        logMessage(jobName, `Transaction record created for ${userEmail}`);
      }

      // Check if notification already exists
      const notificationMessage = `Wallet withdrawal of $${walletBalance.toFixed(
        2
      )} processed successfully.`;
      const existingNotif = await trx.execute(
        dsql`
          SELECT id FROM notifications 
          WHERE userid = ${profileId} 
            AND message = ${notificationMessage} 
          LIMIT 1
        `
      );

      if (existingNotif.rows.length === 0) {
        await trx.execute(
          dsql`
            INSERT INTO notifications (userid, generationid, url, message, type)
            VALUES (
              ${profileId},
              ${"Study Send Inc"},
              ${"/wallet"},
              ${notificationMessage},
              ${"withdrawal"}
            )
          `
        );
        logMessage(jobName, `Notification created for ${userEmail}`);
      }

      // Final step: Update wallet to zero balance and clear status
      // This is the critical step that must only happen after Stripe succeeds
      await trx.execute(
        dsql`
          UPDATE wallet 
          SET 
            amount = 0.0, 
            date = NOW(), 
            withdrawal_status = NULL,
            stripe_transfer_id = NULL
          WHERE id = ${walletId}
        `
      );

      logMessage(jobName, `Wallet balance cleared for ${userEmail}`);
    });

    logMessage(jobName, `✅ Withdrawal completed for ${userEmail}`);
  } catch (err) {
    logMessage(
      jobName,
      `❌ Transfer failed for ${userEmail}: ${err.message}`,
      true
    );

    // Revert withdrawal status on failure (unless Stripe succeeded)
    // If stripe_transfer_id exists, keep 'withdrawing' status for retry
    try {
      const checkTransfer = await db.execute(
        dsql`SELECT stripe_transfer_id FROM wallet WHERE id = ${walletId}`
      );

      if (checkTransfer.rows[0]?.stripe_transfer_id) {
        logMessage(
          jobName,
          `Keeping 'withdrawing' status for ${userEmail} - will retry DB operations`
        );
      } else {
        // No successful Stripe transfer, safe to revert
        await db.execute(
          dsql`
            UPDATE wallet 
            SET withdrawal_status = NULL 
            WHERE id = ${walletId} AND withdrawal_status = 'withdrawing'
          `
        );
        logMessage(jobName, `Reverted withdrawal status for ${userEmail}`);
      }
    } catch (revertError) {
      logMessage(
        jobName,
        `Could not revert status: ${revertError.message}`,
        true
      );
    }
  }
}
