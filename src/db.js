import { sql } from "@vercel/postgres";
import { drizzle } from "drizzle-orm/vercel-postgres";
import dotenv from "dotenv";

dotenv.config();

console.log("this is the database url", process.env);
export const db = drizzle(sql);
