import sqldb from "../mysqldb.js";
import jwt from "jsonwebtoken";
import Joi from "joi";
import bcrypt from "bcrypt";
import { sendEmail } from "../services/ses.js";

export const register = async (req, res) => {
  const connection = await sqldb.getConnection();
  try {
    const registerSchema = Joi.object({
      email: Joi.string().email().required().messages({
        "string.email": "Invalid email format",
        "any.required": "Email is required",
      }),
      password: Joi.string().min(6).pattern(/^\S*$/).required().messages({
        "string.min": "Password must be at least 6 characters long",
        "string.pattern.base": "Password should not contain spaces",
        "any.required": "Password is required",
      }),
    });

    const { email, password } = req.body;

    // Validate input
    const { error } = registerSchema.validate({ email, password });
    if (error) {
      return res.status(400).json({
        status: 400,
        success: false,
        message: error.details[0].message,
      });
    }

    await connection.beginTransaction();

    // Check if email already exists
    const [existingUser] = await connection.execute(
      "SELECT * FROM users WHERE email = ?",
      [email]
    );

    if (existingUser.length > 0) {
      return res.status(409).json({
        status: 409,
        success: false,
        message: "Email already registered",
      });
    }

    // Hash the password
    const hashedPassword = await bcrypt.hash(password, 10);

    // Insert new user
    const [result] = await connection.execute(
      "INSERT INTO users (email, password) VALUES (?, ?)",
      [email, hashedPassword]
    );

    await connection.commit();

    return res.status(201).json({
      status: 201,
      success: true,
      message: "User registered successfully",
      data: {
        user_id: result.insertId,
        email: email,
      },
    });
  } catch (error) {
    await connection.rollback();
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Internal server error",
      error: error.message || error,
    });
  } finally {
    connection.release();
  }
};

export const login = async (req, res) => {
  const connection = await sqldb.getConnection();
  try {
    const loginSchema = Joi.object({
      email: Joi.string().email().required().messages({
        "string.email": "Invalid email format",
        "any.required": "Email is required",
      }),
      password: Joi.string().min(6).pattern(/^\S*$/).required().messages({
        "string.min": "Password must be at least 6 characters long",
        "string.pattern.base": "Password should not contain spaces",
        "any.required": "Password is required",
      }),
    });

    const { email, password } = req.body;

    const { error } = loginSchema.validate({ email, password });
    if (error) {
      return res.status(400).json({
        status: 400,
        success: false,
        message: error.details[0].message,
      });
    }

    await connection.beginTransaction();

    // Check if the user exists
    const [userRows] = await connection.execute(
      "SELECT * FROM users WHERE email = ?",
      [email]
    );

    if (userRows.length === 0) {
      return res.status(401).json({
        status: 401,
        success: false,
        message: "Invalid email or password",
      });
    }

    const { user_id, email: userEmail, password: storedPassword } = userRows[0];

    const isPasswordValid = await bcrypt.compare(password, storedPassword);
    if (!isPasswordValid) {
      return res.status(401).json({
        status: 401,
        success: false,
        message: "Invalid email or password",
      });
    }

    // Generate access and refresh tokens
    const accessToken = jwt.sign(
      { user_id, email: userEmail },
      process.env.ACCESS_TOKEN_SECRET,
      { expiresIn: process.env.ACCESS_TOKEN_EXPIRES_IN }
    );

    const refreshToken = jwt.sign(
      { user_id, email: userEmail },
      process.env.REFRESH_TOKEN_SECRET,
      { expiresIn: process.env.REFRESH_TOKEN_EXPIRES_IN }
    );

    // Calculate expiry times for both tokens
    const accessTokenExpiry = new Date(
      Date.now() + Number(process.env.ACCESS_TOKEN_EXPIRES_IN)
    );
    const refreshTokenExpiry = new Date(
      Date.now() + Number(process.env.REFRESH_TOKEN_EXPIRES_IN)
    );

    // Check if access token exists for user, and update or insert
    const [existingAccessToken] = await connection.execute(
      "SELECT * FROM access_tokens WHERE user_id = ?",
      [user_id]
    );

    if (existingAccessToken.length > 0) {
      await connection.execute(
        "UPDATE access_tokens SET token = ?, expires_at = ? WHERE user_id = ?",
        [accessToken, accessTokenExpiry, user_id]
      );
    } else {
      await connection.execute(
        "INSERT INTO access_tokens (user_id, token, expires_at) VALUES (?, ?, ?)",
        [user_id, accessToken, accessTokenExpiry]
      );
    }

    // Check if refresh token exists for user, and update or insert
    const [existingRefreshToken] = await connection.execute(
      "SELECT * FROM refresh_tokens WHERE user_id = ?",
      [user_id]
    );

    if (existingRefreshToken.length > 0) {
      await connection.execute(
        "UPDATE refresh_tokens SET token = ?, expires_at = ? WHERE user_id = ?",
        [refreshToken, refreshTokenExpiry, user_id]
      );
    } else {
      await connection.execute(
        "INSERT INTO refresh_tokens (user_id, token, expires_at) VALUES (?, ?, ?)",
        [user_id, refreshToken, refreshTokenExpiry]
      );
    }

    await connection.commit();

    return res.status(200).json({
      status: 200,
      success: true,
      data: {
        user_id,
        email: userEmail,
        access_token: accessToken,
        access_token_expiry: accessTokenExpiry,
        refresh_token: refreshToken,
        refresh_token_expiry: refreshTokenExpiry,
      },
    });
  } catch (error) {
    await connection.rollback();
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Internal server error",
      error: error.message || error,
    });
  } finally {
    connection.release(); // Release connection back to pool
  }
};

export const logout = async (req, res) => {
  const connection = await sqldb.getConnection();
  try {
    const userId = req.user.user_id;

    await connection.beginTransaction();

    // Delete access token
    await connection.execute("DELETE FROM access_tokens WHERE user_id = ?", [
      userId,
    ]);

    // Delete refresh token
    await connection.execute("DELETE FROM refresh_tokens WHERE user_id = ?", [
      userId,
    ]);

    await connection.commit();

    return res.status(200).json({
      status: 200,
      success: true,
      message: "Successfully logged out",
    });
  } catch (error) {
    await connection.rollback();
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Internal server error",
      error: error.message || error,
    });
  } finally {
    connection.release(); // release the DB connection back to pool
  }
};

export const refreshToken = async (req, res) => {
  const { refresh_token } = req.body;

  if (!refresh_token) {
    return res.status(400).json({
      status: 400,
      success: false,
      message: "Refresh token is required",
    });
  }

  const connection = await sqldb.getConnection();

  try {
    await connection.beginTransaction();

    const decoded = jwt.verify(refresh_token, process.env.REFRESH_TOKEN_SECRET);
    const userId = decoded.user_id;

    const [rows] = await connection.execute(
      "SELECT * FROM refresh_tokens WHERE user_id = ? AND token = ?",
      [userId, refresh_token]
    );

    if (rows.length === 0) {
      await connection.rollback();
      return res.status(401).json({
        status: 401,
        success: false,
        message: "Refresh token is not valid or not found",
      });
    }

    const tokenRecord = rows[0];
    const now = new Date();
    const expiry = new Date(tokenRecord.expires_at);

    if (now >= expiry) {
      await connection.rollback();
      return res.status(401).json({
        status: 401,
        success: false,
        message: "Refresh token has expired",
      });
    }

    const newAccessToken = jwt.sign(
      { user_id: userId, email: decoded.email },
      process.env.ACCESS_TOKEN_SECRET,
      { expiresIn: process.env.ACCESS_TOKEN_EXPIRES_IN }
    );

    const newAccessTokenExpiry = new Date(
      Date.now() + Number(process.env.ACCESS_TOKEN_EXPIRES_IN)
    );

    const [existingAccess] = await connection.execute(
      "SELECT * FROM access_tokens WHERE user_id = ?",
      [userId]
    );

    if (existingAccess.length > 0) {
      await connection.execute(
        "UPDATE access_tokens SET token = ?, expires_at = ? WHERE user_id = ?",
        [newAccessToken, newAccessTokenExpiry, userId]
      );
    } else {
      await connection.execute(
        "INSERT INTO access_tokens (user_id, token, expires_at) VALUES (?, ?, ?)",
        [userId, newAccessToken, newAccessTokenExpiry]
      );
    }

    await connection.commit();

    return res.status(200).json({
      status: 200,
      success: true,
      access_token: newAccessToken,
      access_token_expiry: newAccessTokenExpiry,
    });
  } catch (err) {
    await connection.rollback();
    return res.status(403).json({
      status: 403,
      success: false,
      message: "Invalid or expired refresh token",
      error: err.message,
    });
  } finally {
    connection.release();
  }
};

export const getProfile = async (req, res) => {
  const { user_id, email } = req.user; // from middleware

  const connection = await sqldb.getConnection();

  try {
    await connection.beginTransaction();

    // Fetch user details from the database using the user_id
    const [userRows] = await connection.execute(
      "SELECT user_id, email, created_at FROM users WHERE user_id = ?",
      [user_id]
    );

    // If the user doesn't exist
    if (userRows.length === 0) {
      return res.status(404).json({
        status: 404,
        success: false,
        message: "User not found",
      });
    }

    const user = userRows[0];

    // Commit the transaction (if used)
    await connection.commit();

    return res.status(200).json({
      status: 200,
      success: true,
      data: {
        user_id: user.user_id,
        email: user.email,
        created_at: user.created_at,
      },
    });
  } catch (error) {
    // Rollback in case of error
    await connection.rollback();
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Internal server error",
      error: error.message || error,
    });
  } finally {
    connection.release(); // Release connection back to pool
  }
};

export const getRedirectLink = async (req, res) => {
  const connection = await sqldb.getConnection();

  try {
    const schema = Joi.object({
      email: Joi.string().email().required(),
    });

    const { email } = req.body;
    const { error } = schema.validate({ email });

    if (error) {
      return res.status(400).json({
        status: 400,
        success: false,
        message: error.details[0].message,
      });
    }

    // Check if user exists
    const [rows] = await connection.execute(
      `SELECT user_id FROM users WHERE email = ?`,
      [email]
    );

    if (rows.length === 0) {
      return res.status(404).json({
        status: 404,
        success: false,
        message: "User not found with this email",
      });
    }

    const { user_id } = rows[0];

    // Generate token
    const resetToken = jwt.sign(
      { user_id, email },
      process.env.RESET_PASSWORD_TOKEN_SECRET,
      { expiresIn: process.env.RESET_PASSWORD_TOKEN_EXPIRES_IN }
    );

    // Set the expiry time for the token
    const expiresAt = new Date(
      Date.now() + parseInt(process.env.RESET_PASSWORD_TOKEN_EXPIRES_IN)
    );

    // Check if a reset record already exists
    const [existing] = await connection.execute(
      `SELECT id FROM reset_passwords WHERE user_id = ?`,
      [user_id]
    );

    if (existing.length > 0) {
      // Update existing reset token
      await connection.execute(
        `UPDATE reset_passwords SET reset_token = ?, expires_at = ? WHERE user_id = ?`,
        [resetToken, expiresAt, user_id]
      );
    } else {
      // Insert new reset token
      await connection.execute(
        `INSERT INTO reset_passwords (user_id, reset_token, expires_at) VALUES (?, ?, ?)`,
        [user_id, resetToken, expiresAt]
      );
    }

    console.log("reset token is", resetToken);

    // Generate reset link
    const resetLink = `${process.env.FRONTEND_RESET_PASSWORD_URL}?token=${resetToken}`;

    // Send email using the imported SES service
    await sendEmail({
      to: email,
      subject: "Password Reset Link",
      html: `
        <p>You requested a password reset.</p>
        <p>Click the link below to reset your password (valid for 15 minutes):</p>
        <a href="${resetLink}">${resetLink}</a>
      `,
    });

    return res.status(200).json({
      status: 200,
      success: true,
      message: "Password reset link sent successfully",
    });
  } catch (error) {
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Internal server error",
      error: error.message || error,
    });
  } finally {
    connection.release();
  }
};

export const resetPassword = async (req, res) => {
  const connection = await sqldb.getConnection();

  try {
    const schema = Joi.object({
      password: Joi.string().min(6).required(),
    });

    const { password } = req.body;
    const { token } = req.query;

    const { error } = schema.validate({ password });
    if (error) {
      return res.status(400).json({
        status: 400,
        success: false,
        message: error.details[0].message,
      });
    }

    // Verify token
    let decoded;
    try {
      decoded = jwt.verify(token, process.env.RESET_PASSWORD_TOKEN_SECRET);
    } catch (err) {
      return res.status(401).json({
        status: 401,
        success: false,
        message: "Invalid or expired token",
      });
    }

    const { user_id } = decoded;

    // Check if token exists in DB and not expired
    const [rows] = await connection.execute(
      `SELECT * FROM reset_passwords WHERE user_id = ? AND reset_token = ?`,
      [user_id, token]
    );

    if (rows.length === 0) {
      return res.status(400).json({
        status: 400,
        success: false,
        message: "Invalid or expired reset token",
      });
    }

    const currentTime = new Date();
    const expiresAt = new Date(rows[0].expires_at);
    if (currentTime > expiresAt) {
      return res.status(400).json({
        status: 400,
        success: false,
        message: "Token has expired",
      });
    }

    // Hash the new password
    const hashedPassword = await bcrypt.hash(password, 10);

    // Update password in users table
    await connection.execute(
      `UPDATE users SET password = ? WHERE user_id = ?`,
      [hashedPassword, user_id]
    );

    // Delete reset token record after successful password reset
    await connection.execute(`DELETE FROM reset_passwords WHERE user_id = ?`, [
      user_id,
    ]);

    return res.status(200).json({
      status: 200,
      success: true,
      message: "Password has been reset successfully",
    });
  } catch (error) {
    console.error("Reset password error:", error);
    return res.status(500).json({
      status: 500,
      success: false,
      message: "Internal server error",
      error: error.message || error,
    });
  } finally {
    connection.release(); // ✅ Release DB connection
  }
};
