import { db } from "../db.js";
import { sql as dsql } from "drizzle-orm";
import { logMessage } from "../logger.js";

export async function updateBookings() {
  const jobName = "Mark-Completed-Bookings";
  try {
    logMessage(jobName, "Date and time: " + new Date().toISOString());
    logMessage(jobName, "Starting booking update process...");
    console.log(`[${jobName}] Starting booking update process...`);

    // todo: only fetch the dates which are one day before do not process all the transactions
    const bookingsQuery = dsql`
      SELECT id, meeting_status, joined_by, payment, status, starts_on
      FROM bookings 
      WHERE transaction_status = 'unavailable' and payment=true;
    `;

    console.log(
      `[${jobName}] Executing bookings query:`,
      bookingsQuery.text || bookingsQuery
    );
    const bookings = await db.execute(bookingsQuery);

    console.log(`[${jobName}] Query complete. Raw result:`, bookings);
    console.log(`[${jobName}] this is the bookings`, bookings.rows);

    if (bookings.rows.length > 0) {
      console.log(
        `[${jobName}] Found ${bookings.rows.length} bookings. Processing each in separate transaction...`
      );

      let successCount = 0;
      let failureCount = 0;

      for (const booking of bookings.rows) {
        try {
          await db.transaction(async (trx) => {
            console.log(
              `[${jobName}] Transaction started for booking ${booking.id}`
            );
            console.log(`[${jobName}] processing this booking`, booking);

            const {
              id,
              meeting_status,
              joined_by,
              payment,
              status,
              starts_on,
            } = booking;
            console.log(
              `[${jobName}] Booking details -> id: ${id}, meeting_status: ${meeting_status}, joined_by: ${joined_by}, payment: ${payment}, status: ${status}, starts_on: ${starts_on}`
            );

            if (payment === true) {
              console.log(`[${jobName}] Payment is true for booking ${id}`);
              if (status === "declined") {
                logMessage(
                  jobName,
                  `Processing Booking ID: ${id} -> status: ${status}, meeting_status: ${meeting_status}`
                );
                console.log(
                  `[${jobName}] Booking ${id} status is declined. Preparing to update transaction_status to 'processRefund'.`
                );

                const updateQuery = dsql`UPDATE bookings 
                     SET transaction_status = 'processRefund' 
                     WHERE id = ${id}`;
                console.log(
                  `[${jobName}] Executing update:`,
                  updateQuery.text || updateQuery
                );
                await trx.execute(updateQuery);
                console.log(
                  `[${jobName}] Update complete for booking ${id}: transaction_status -> processRefund`
                );

                logMessage(
                  jobName,
                  `Updated Booking ID: ${id} -> transaction_status: processRefund`
                );
                return;
              }
              console.log(
                `[${jobName}] event was not declined for booking ${id}`
              );

              // this should be separate
              if (
                joined_by &&
                (meeting_status === "created" || meeting_status === "completed")
              ) {
                logMessage(
                  jobName,
                  `Processing Booking ID: ${id} -> meeting_status: ${meeting_status}, joined_by: ${joined_by}`
                );
                console.log(
                  `[${jobName}] Booking ${id} has joined_by and meeting_status in [created, completed].`
                );

                let newMeetingStatus =
                  meeting_status === "created" ? "completed" : meeting_status;

                const updateQuery = dsql`UPDATE bookings 
                     SET meeting_status = ${newMeetingStatus}, 
                         transaction_status = 'processing' 
                     WHERE id = ${id}`;
                console.log(
                  `[${jobName}] Executing update:`,
                  updateQuery.text || updateQuery
                );
                await trx.execute(updateQuery);
                console.log(
                  `[${jobName}] Updated Booking ID: ${id} -> meeting_status: ${newMeetingStatus}, transaction_status: processing`
                );

                logMessage(
                  jobName,
                  `Updated Booking ID: ${id} -> meeting_status: ${newMeetingStatus}, transaction_status: processing`
                );
              } else {
                logMessage(
                  jobName,
                  `Marked refund for Booking ID: ${id} -> meeting_status: ${meeting_status}, joined_by: ${joined_by}`
                );
                console.log(
                  `[${jobName}] Booking ${id} does not meet join/meeting criteria. Considering refund. meeting_status: ${meeting_status}, joined_by: ${joined_by}`
                );

                // TODO check the date if the date is passed then refund
                // has to be checked

                // starts_on is a timestamp in utc  check with the current date  and if it is more than 12 hr then it will update the trancaction
                const currentDate = new Date();
                const startsOnDate = new Date(starts_on);
                const diffInHours =
                  (currentDate - startsOnDate) / (1000 * 60 * 60);

                console.log(
                  `[${jobName}] Booking ${id} starts_on: ${startsOnDate.toISOString()}, currentDate: ${currentDate.toISOString()}, diffInHours: ${diffInHours}`
                );

                if (diffInHours > 12) {
                  const updateQuery = dsql`UPDATE bookings 
                       SET transaction_status = 'processRefund' 
                       WHERE id = ${id}`;
                  console.log(
                    `[${jobName}] Diff > 12 hours for booking ${id}. Executing update:`,
                    updateQuery.text || updateQuery
                  );
                  await trx.execute(updateQuery);
                  console.log(
                    `[${jobName}] Update complete for booking ${id}: transaction_status -> processRefund`
                  );
                } else {
                  console.log(
                    `[${jobName}] Diff <= 12 hours for booking ${id}. No transaction_status change.`
                  );
                }
              }
            } else {
              console.log(
                `[${jobName}] Payment is not true for booking ${id}. Skipping.`
              );
            }
            console.log(`[${jobName}] Transaction complete for booking ${id}.`);
          });
          successCount++;
        } catch (error) {
          failureCount++;
          logMessage(
            jobName,
            `Error processing booking ${booking.id}: ${error.message}`,
            true
          );
          console.error(
            `[${jobName}] Error processing booking ${booking.id}:`,
            error
          );
          // Continue processing other bookings
        }
      }

      logMessage(
        jobName,
        `Booking update complete. Success: ${successCount}, Failed: ${failureCount}`
      );
      console.log(
        `[${jobName}] Booking update complete. Success: ${successCount}, Failed: ${failureCount}`
      );
    } else {
      logMessage(jobName, "No bookings found for processing.");
      console.log(`[${jobName}] No bookings found for processing.`);
    }
  } catch (error) {
    logMessage(jobName, `Error updating bookings: ${error.message}`, true);
    console.error(`[${jobName}] Error updating bookings:`, error);
  }
}
