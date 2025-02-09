import { updateBookings } from "./aUpdateBookings.js";
updateBookings()
  .then(() => console.log("Update completed."))
  .catch((err) => console.error("Error:", err));
