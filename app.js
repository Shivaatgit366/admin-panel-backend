import express from "express";
import cors from "cors";
import helmet from "helmet";

import { globalErrorHandler } from "./controllers/errorController.js";
import stullerRoutes from "./routes/stullerRoutes.js";
import homePageRoutes from "./routes/homepageRoutes.js";
import authRoutes from "./routes/authRoutes.js";
import webhookRoutes from "./routes/webhookCallbackRoutes.js";
import compression from "compression";

const app = express();

app.use(helmet());

// Middleware and CORS setup (unchanged)
const allowedOrigins = new Set([
  "http://localhost:3000",
  "http://localhost:5173",
  "https://wbsbk.chicago-jewelers.com",
  "https://wbsfr.chicago-jewelers.com",
]);

const corsOptions = {
  origin: (origin, callback) => {
    if (!origin || allowedOrigins.has(origin)) {
      return callback(null, true); // allow undefined origin (Postman)
    }

    console.warn(
      `❌ Blocked CORS request from origin: ${origin} | Time: ${new Date().toLocaleString()}`
    );

    // CORS expects a regular error, not a custom one
    callback(new Error("Not allowed by CORS"));
  },
  optionsSuccessStatus: 200,
  credentials: true,
};

app.use(cors(corsOptions));
app.use(express.json());
app.use(compression());

// Routes
app.use("/api/v1/band-stuller", stullerRoutes);
app.use("/api/v1/auth", authRoutes);
app.use("/api/v1/homepage", homePageRoutes);
app.use("/api/v1", webhookRoutes);
app.use("/uploads", express.static("uploads"));

// Global error handler
app.all("*", (req, res) => {
  res.status(404).json({
    status: 404,
    success: false,
    message: `Can't find ${req.originalUrl} on the server!`,
  });
});

app.use(globalErrorHandler);

export default app;
