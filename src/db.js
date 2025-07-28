// db.ts
import dotenv from "dotenv";
import { drizzle } from "drizzle-orm/node-postgres"; // or "drizzle-orm/neon-serverless"
import { Pool as PgPool } from "pg";
import { Pool as NeonPool, neonConfig } from "@neondatabase/serverless";
import ws from "ws";

dotenv.config();

const isNeon = process.env.DATABASE_URL?.includes("neon.tech");

export const db = isNeon
  ? (() => {
      neonConfig.webSocketConstructor = ws;
      const neon = new NeonPool({ connectionString: process.env.DATABASE_URL });
      return drizzle(neon); // use drizzle wrapper
    })()
  : (() => {
      const pg = new PgPool({ connectionString: process.env.DATABASE_URL });
      return drizzle(pg); // use drizzle wrapper
    })();
