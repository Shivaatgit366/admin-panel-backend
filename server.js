//env import
import dotenv from "dotenv";
dotenv.config();
import mongoose from "mongoose";
import "./services/cron.js";
import { Server } from "socket.io";
import http from "http";
import jwt from "jsonwebtoken";
import sqldb from "./mysqldb.js";
import { registerSocketIO } from "./services/socketEvents.js";

// process.on('uncaughtException', (err) => {
//     console.log(err.name, err.message);
//     console.log('Uncaught Exception occured! Shutting down... vishnoi');
//     process.exit(1);
// })

import app from "./app.js";

/**
 * HTTP server and Websocket server related code
 */
// Setup the HTTP server on top of the express server and attach the WebSocket server to it
const server = http.createServer(app);

// Initialize Socket.io with the HTTP server
const io = new Server(server, {
  cors: {
    origin: "*",
  },
});

// Register the io instance
registerSocketIO(io);

// WebSocket connection handler
io.on("connection", async (socket) => {
  console.log("New connection established:", socket.id);

  const disconnectWithError = (message) => {
    socket.emit("auth_error", message);
    // Give the client a moment to receive the message before disconnecting
    setTimeout(() => {
      socket.disconnect(true);
    }, 2000); // 2s delay to ensure delivery
  };

  // Token validation logic after connection
  const authHeader = socket.handshake.headers?.authorization;
  const token = authHeader && authHeader.split(" ")[1];

  if (!token) {
    console.log("Access token missing");
    return disconnectWithError("Access token missing");
  }

  let decoded;
  try {
    decoded = jwt.verify(token, process.env.ACCESS_TOKEN_SECRET);
  } catch (err) {
    console.log("Error verifying token:", err.message);
    return disconnectWithError("Invalid or expired access token");
  }

  const connection = await sqldb.getConnection();

  try {
    const [rows] = await connection.execute(
      `SELECT * FROM access_tokens WHERE user_id = ? AND token = ?`,
      [decoded.user_id, token]
    );

    if (rows.length === 0) {
      console.log("Token not found in the system");
      return disconnectWithError("Token not found in the system");
    }

    const tokenRecord = rows[0];
    const expiryTime = new Date(tokenRecord.expires_at + "Z"); // UTC
    const now = new Date();

    if (now >= expiryTime) {
      console.log("Token has expired in the system");
      return disconnectWithError("Token has expired in the system");
    }

    // Attach user info to socket
    socket.user_id = decoded.user_id;
    socket.email = decoded.email;

    console.log("User connected:", socket.user_id, socket.email);

    socket.emit("response", {
      message: "Connection established",
      userData: `userId is ${socket.user_id}, user email is ${socket.email}`,
    });

    // Now you can handle other events or messages as needed
    socket.on("message", (data) => {
      console.log("Received message:", data);
      socket.emit("response", { message: "Message received" });
    });
  } catch (err) {
    console.log("Error", err.message);
    return disconnectWithError("Internal server error");
  } finally {
    connection.release();
  }

  // Handle disconnect event
  socket.on("disconnect", () => {
    console.log("User disconnected:", socket.id);
  });
});

// connect the mongodb database
mongoose
  .connect(process.env.MONGO_URL)
  .then((conn) => {
    //console.log(conn);
    console.log("DB Connection Successful");
  })
  .catch((err) => {
    // Connection error
    console.error("Error connecting to the database:", err.message);
  });

const PORT = process.env.PORT || 8000;

// this starts both Express and WebSocket server
server.listen(PORT, () => {
  console.log(`Express + WebSocket server Running on the port ${PORT}`);
});

process.on("unhandledRejection", (err) => {
  console.log(err.name, err.message);
  console.log("Unhandled rejection occured! Shutting down...");
});
