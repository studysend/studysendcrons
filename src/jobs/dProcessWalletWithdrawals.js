import { db } from "../db.js";
import { sql as dsql } from "drizzle-orm";
import Stripe from "stripe";
import { logMessage } from "../logger.js";

export async function processWalletWithdrawals() {
  const stripe = new Stripe(process.env.STRIPE_SECRET.trim());
  const jobName = "wallet-withdrawals";

  try {
    logMessage(jobName, "Starting wallet withdrawal process...");

    // Fetch all eligible wallets first (outside any transaction)
    const walletRows = await db.execute(
      dsql`SELECT email, amount FROM wallet WHERE amount >= 10 AND status = 'active'`
    );

    if (walletRows.rows.length === 0) {
      logMessage(jobName, "No eligible wallets found for transfer.");
      return;
    }

    for (const wallet of walletRows.rows) {
      const userEmail = wallet.email;
      const walletBalance = parseFloat(wallet.amount);

      logMessage(jobName, `Processing ${userEmail} with $${walletBalance}`);

      // Fetch Stripe account ID and profile ID
      const profileData = await db.execute(
        dsql`SELECT id, stripe_account_id FROM profile WHERE email = ${userEmail}`
      );

      if (
        profileData.rows.length === 0 ||
        !profileData.rows[0].stripe_account_id
      ) {
        logMessage(jobName, `No Stripe account for ${userEmail}`, true);
        continue;
      }

      const profileId = profileData.rows[0].id;
      const stripeAccountId = profileData.rows[0].stripe_account_id;

      try {
        // 1️⃣ Transfer money via Stripe (external operation)
        const transfer = await stripe.transfers.create({
          amount: Math.round(walletBalance * 100),
          currency: "usd",
          destination: stripeAccountId,
        });

        logMessage(
          jobName,
          `Transfer successful for ${userEmail}: ${transfer.id}`
        );
        console.log("this is the transfer", transfer);

        // if transfer is successful, proceed to update DB
        if (transfer.id && transfer.amount) {
          await db.transaction(async (trx) => {
            await trx.execute(
              dsql`UPDATE wallet SET amount = 0.0, date = NOW() WHERE email = ${userEmail}`
            );

            await trx.execute(
              dsql`INSERT INTO transactions (type, amount, email, "to", "from", message, transaction_id)
              VALUES (
                ${"credit"},
                ${walletBalance}, 
                ${userEmail},
                ${stripeAccountId},
                ${"company_account"},
                ${`Transferred ${walletBalance} to Stripe account ${stripeAccountId}`},
                ${`to_stripe_${transfer.id}`}
              )`
            );

            // insert the notification
            await trx.execute(
              dsql`INSERT INTO notifications (userid, generationid, url, message, type)
              VALUES (
                ${profileId},
                ${"Study Send Inc"},
                ${"/"},
                ${`Wallet withdrawal of ${walletBalance} processed successfully.`},
                ${"refund"}
              )`
            );
          });

          logMessage(jobName, `Wallet updated for ${userEmail}`);
        }
      } catch (err) {
        logMessage(
          jobName,
          `Transfer failed for ${userEmail}: ${err.message}`,
          true
        );
      }
    }

    logMessage(jobName, "All wallet withdrawals processed ✅");
  } catch (error) {
    logMessage(jobName, `Critical error: ${error.message}`, true);
  }
}
