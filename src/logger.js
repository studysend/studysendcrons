import fs from "fs";
import path from "path";

const logDir = "./logs";
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
}

export function logMessage(jobName, message, isError = false) {
  const logFile = path.join(logDir, `${jobName}.log`);
  const logEntry = `[${new Date().toISOString()}] ${message}\n`;

  fs.appendFileSync(logFile, logEntry, "utf8");

  if (isError) {
    console.error(logEntry);
  } else {
    console.log(logEntry);
  }
}
