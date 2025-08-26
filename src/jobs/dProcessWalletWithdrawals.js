import { db } from "../db.js";
import { sql as dsql } from "drizzle-orm";
import Stripe from "stripe";
import { logMessage } from "../logger.js";

export async function processWalletWithdrawals() {
  const stripe = new Stripe(process.env.STRIPE_SECRET);

  const jobName = "wallet-withdrawals";
  try {
    logMessage(jobName, "Starting wallet withdrawal process...");

    // Begin a transaction
    await db.transaction(async (trx) => {
      // Fetch all wallet rows with an amount > 10 and active status
      const walletRows = await trx.execute(
        dsql`SELECT email, amount FROM wallet WHERE amount > 10 AND status = 'active'`
      );

      if (walletRows.rows.length === 0) {
        logMessage(jobName, "No eligible wallets found for transfer.");
        return;
      }

      for (const wallet of walletRows.rows) {
        console.log("processing for the wallet", wallet);
        const userEmail = wallet.email;
        const walletBalance = parseFloat(wallet.amount);

        logMessage(
          jobName,
          `Processing wallet for ${userEmail} with balance: ${walletBalance}`
        );

        // Fetch the user's connected Stripe account ID
        const profileData = await trx.execute(
          dsql`SELECT stripe_account_id FROM profile WHERE email = ${userEmail}`
        );

        if (
          profileData.rows.length === 0 ||
          !profileData.rows[0].stripe_account_id
        ) {
          logMessage(
            jobName,
            `Stripe account not found for ${userEmail}. Skipping.`,
            true
          );
          continue;
        }

        const stripeAccountId = profileData.rows[0].stripe_account_id;

        try {
          // Transfer funds from company account to user’s Stripe account
          const transfer = await stripe.transfers.create({
            amount: Math.round(walletBalance * 100), // Convert to smallest currency unit
            currency: "usd",
            destination: stripeAccountId,
          });

          logMessage(
            jobName,
            `Transfer successful for ${userEmail}: ${transfer.id}`
          );

          // Deduct amount from user's wallet
          await trx.execute(
            dsql`UPDATE wallet SET amount = 0.0, date = NOW() WHERE email = ${userEmail}`
          );

          // Log transaction
          await trx.execute(
            dsql`INSERT INTO transactions (type, amount, email, "to", "from", message, transaction_id)
              VALUES (
                ${"credit"},
                ${walletBalance}, 
                ${userEmail},
                ${stripeAccountId},
                ${"company_account"},
                ${`Transferred $${walletBalance} to Stripe account ${stripeAccountId}`},
                ${`to_stripe_${transfer.id}`}
              )`
          );

          logMessage(
            jobName,
            `Transaction logged successfully for ${userEmail}`
          );
        } catch (stripeError) {
          logMessage(
            jobName,
            `Failed to transfer funds for ${userEmail}: ${stripeError.message}`,
            true
          );
          throw new Error(
            `Transaction failed for ${userEmail}: ${stripeError.message}`
          );
        }
      }
    });

    logMessage(jobName, "Wallet withdrawals processed successfully.");
  } catch (error) {
    logMessage(
      jobName,
      `Error processing wallet withdrawals: ${error.message}`,
      true
    );
  }
}
