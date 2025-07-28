import { db } from "../db.js";
import { sql as dsql } from "drizzle-orm";
import { logMessage } from "../logger.js";

export async function updateBookings() {
  const jobName = "Mark-Completed-Bookings";
  try {
    logMessage(jobName, "Starting booking update process...");

    // todo: only fetch the dates which are one day before do not process all the transactions
    const bookingsQuery = dsql`
      SELECT id, meeting_status, joined_by 
      FROM bookings 
      WHERE transaction_status = 'unavailable' and payment=true;
    `;

    const bookings = await db.execute(bookingsQuery);

    if (bookings.rows.length > 0) {
      await db.transaction(async (trx) => {
        for (const booking of bookings.rows) {
          const { id, meeting_status, joined_by, payment } = booking;
          if (payment === true) {
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
              await trx.execute(
                dsql`UPDATE bookings 
                   SET transaction_status = 'processRefund' 
                   WHERE id = ${id}`
              );
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
