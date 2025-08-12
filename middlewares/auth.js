import sqldb from "../mysqldb.js";
import jwt from "jsonwebtoken";

export const authenticateToken = async (req, res, next) => {
  const authHeader = req.headers["authorization"];
  const token = authHeader && authHeader.split(" ")[1];

  if (!token) {
    return res.status(401).json({
      status: 401,
      success: false,
      message: "Access token missing",
    });
  }

  let decoded;
  try {
    decoded = jwt.verify(token, process.env.ACCESS_TOKEN_SECRET);
  } catch (err) {
    return res.status(403).json({
      status: 403,
      success: false,
      message: "Invalid or expired access token",
    });
  }

  const connection = await sqldb.getConnection();

  try {
    const [rows] = await connection.execute(
      `SELECT * FROM access_tokens WHERE user_id = ? AND token = ?`,
      [decoded.user_id, token]
    );

    if (rows.length === 0) {
      return res.status(401).json({
        status: 401,
        success: false,
        message: "Token not found in the system",
      });
    }

    const tokenRecord = rows[0];
    const expiryTime = new Date(tokenRecord.expires_at + "Z"); // Force UTC
    const now = new Date();

    if (now >= expiryTime) {
      return res.status(401).json({
        status: 401,
        success: false,
        message: "Token has expired in the system",
      });
    }

    req.user = decoded;
    next();
  } catch (err) {
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Internal server error",
      error: err.message,
    });
  } finally {
    connection.release();
  }
};
