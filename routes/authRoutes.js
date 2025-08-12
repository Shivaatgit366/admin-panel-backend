import express from "express";
import {
  register,
  login,
  logout,
  refreshToken,
  getProfile,
  getRedirectLink,
  resetPassword,
} from "../controllers/authController.js";
import { authenticateToken } from "../middlewares/auth.js";

//router object
const router = express.Router();

//group
router.post("/register", register);
router.post("/login", login);
router.post("/logout", authenticateToken, logout);
router.post("/refresh-token", refreshToken);
router.get("/profile", authenticateToken, getProfile);
router.post("/send-reset-link", getRedirectLink);
router.post("/reset-password", resetPassword);

export default router;
