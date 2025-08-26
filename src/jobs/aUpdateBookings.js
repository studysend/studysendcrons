import { db } from "../db.js";
import { sql as dsql } from "drizzle-orm";
import { logMessage } from "../logger.js";

export async function updateBookings() {
  const jobName = "Mark-Completed-Bookings";
  try {
    logMessage(jobName, "Starting booking update process...");

    // todo: only fetch the dates which are one day before do not process all the transactions
    const bookingsQuery = dsql`
      SELECT id, meeting_status, joined_by, payment, status, starts_on
      FROM bookings 
      WHERE transaction_status = 'unavailable' and payment=true;
    `;

    const bookings = await db.execute(bookingsQuery);

    console.log("this is the bookings", bookings.rows);

    if (bookings.rows.length > 0) {
      await db.transaction(async (trx) => {
        for (const booking of bookings.rows) {
          console.log("processing this booking", booking);
          const { id, meeting_status, joined_by, payment, status } = booking;
          if (payment === true) {
            if (status === "declined") {
              logMessage(
                jobName,
                `Processing Booking ID: ${id} -> status: ${status}, meeting_status: ${meeting_status}`
              );
              await trx.execute(
                dsql`UPDATE bookings 
                   SET transaction_status = 'processRefund' 
                   WHERE id = ${id}`
              );
              logMessage(
                jobName,
                `Updated Booking ID: ${id} -> transaction_status: processRefund`
              );
              continue;
            }
            console.log("event was not declined");

            // this should be separate
            if (
              joined_by &&
              (meeting_status === "created" || meeting_status === "completed")
            ) {
              logMessage(
                jobName,
                `Processing Booking ID: ${id} -> meeting_status: ${meeting_status}, joined_by: ${joined_by}`
              );
              let newMeetingStatus =
                meeting_status === "created" ? "completed" : meeting_status;

              await trx.execute(
                dsql`UPDATE bookings 
                   SET meeting_status = ${newMeetingStatus}, 
                       transaction_status = 'processing' 
                   WHERE id = ${id}`
              );

              logMessage(
                jobName,
                `Updated Booking ID: ${id} -> meeting_status: ${newMeetingStatus}, transaction_status: processing`
              );
            } else {
              logMessage(
                jobName,
                `Maked refund for Booking ID: ${id} -> meeting_status: ${meeting_status}, joined_by: ${joined_by}`
              );

              // TODO check the date if the date is passed then refund
              // has to be checked

              // starts_on is a timestamp in utc  check with the current date  and if it is more than 12 hr then it will update the trancaction
              const currentDate = new Date();
              const startsOnDate = new Date(starts_on);
              const diffInHours =
                (currentDate - startsOnDate) / (1000 * 60 * 60);

              if (diffInHours > 12) {
                await trx.execute(
                  dsql`UPDATE bookings 
                     SET transaction_status = 'processRefund' 
                     WHERE id = ${id}`
                );
              }
            }
          }
        }
      });

      logMessage(jobName, "All eligible bookings updated successfully on.");
    } else {
      logMessage(jobName, "No bookings found for processing.");
    }
  } catch (error) {
    logMessage(jobName, `Error updating bookings: ${error.message}`, true);
  }
}
