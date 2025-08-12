import express from "express";
import { insertStullerInDb } from "../controllers/stullerController.js";
import {
  createGroup,
  getCategoryList,
  getGroup,
  getGroupsByIds,
  syncCategory,
  updateSingleGroup,
  deleteSingleGroup,
} from "../controllers/stullerGroupController.js";

import {
  createMetal,
  getMetal,
  getMetalsByIds,
  updateSingleMetal,
  deleteSingleMetal,
} from "../controllers/stullerMetalController.js";

import {
  createShape,
  getShape,
  getShapesByIds,
  updateSingleShape,
  deleteSingleShape,
} from "../controllers/stullerShapeController.js";

import {
  createStyle,
  getStyle,
  getStylesByIds,
  updateSingleStyle,
  deleteSingleStyle,
} from "../controllers/stullerStyleController.js";

import {
  assignedGroup,
  allAssignedGroups,
  assignedGroupById,
  assignGroup,
  assignGroupEdit,
} from "../controllers/stullerGroupSyncController.js";
import {
  syncProduct,
  asyncProduct,
  deleteShopifyProduct,
  editProduct,
  getOneProduct,
  getAllProducts,
  productActionsInBulk,
  getAllProductsCount,
  sendRegistrationEmail,
  updateEngagementProduct,
} from "../controllers/stullerProductSyncController.js";

import { authenticateToken } from "../middlewares/auth.js";
import uploadFile from "../middlewares/multer.js";
import resizeAndCompressFile from "../middlewares/sharp.js";

//router object
const router = express.Router();
router.get("/insert-supplier-db", insertStullerInDb);

//group
router.post("/group", authenticateToken, getGroup);
router.post("/group/get-by-ids", authenticateToken, getGroupsByIds);
router.post("/group/create", authenticateToken, createGroup);
router.put("/group/:id", authenticateToken, updateSingleGroup);
router.delete("/group/:id", authenticateToken, deleteSingleGroup);

//metal
router.post("/metal", authenticateToken, getMetal);
router.post("/metal/get-by-ids", authenticateToken, getMetalsByIds);
router.post(
  "/metal/create",
  authenticateToken,
  uploadFile,
  resizeAndCompressFile,
  createMetal
);
router.put(
  "/metal/:id",
  authenticateToken,
  uploadFile,
  resizeAndCompressFile,
  updateSingleMetal
);
router.delete("/metal/:id", authenticateToken, deleteSingleMetal);

//shape
router.post("/shape", authenticateToken, getShape);
router.post("/shape/get-by-ids", authenticateToken, getShapesByIds);
router.post(
  "/shape/create",
  authenticateToken,
  uploadFile,
  resizeAndCompressFile,
  createShape
);
router.put(
  "/shape/:id",
  authenticateToken,
  uploadFile,
  resizeAndCompressFile,
  updateSingleShape
);
router.delete("/shape/:id", authenticateToken, deleteSingleShape);

//style
router.post("/style", authenticateToken, getStyle);
router.post("/style/get-by-ids", authenticateToken, getStylesByIds);
router.post(
  "/style/create",
  authenticateToken,
  uploadFile,
  resizeAndCompressFile,
  createStyle
);
router.put(
  "/style/:id",
  authenticateToken,
  uploadFile,
  resizeAndCompressFile,
  updateSingleStyle
);
router.delete("/style/:id", authenticateToken, deleteSingleStyle);

//sync category with shopify
router.get("/sync-category", authenticateToken, syncCategory);
router.get("/category-list", authenticateToken, getCategoryList);

// asing group to stuller group id
router.get("/assign-group", authenticateToken, assignGroup); // get all the un-assigned groups
router.post("/assigned-group", authenticateToken, assignedGroup); // get all the assigned groups
router.get("/assigned-group", authenticateToken, allAssignedGroups); // get all the assigned groups
router.get("/assigned-group/:id", authenticateToken, assignedGroupById); // get particular assigned group
router.post("/assign-group/edit", authenticateToken, assignGroupEdit); // assign or edit

//sync product with shopify
router.post("/sync-product", authenticateToken, syncProduct); // put the product from db into shopify
router.post("/async-product", authenticateToken, asyncProduct); // put the shopify product into "archive"
router.post("/delete-product", authenticateToken, deleteShopifyProduct); // delete the shopify product through the API and delete in local db also
router.post(
  "/product-actions-in-bulk",
  authenticateToken,
  productActionsInBulk
); // products actions in bulk
router.post("/edit-product", authenticateToken, editProduct); // edit the fields of a particular product
router.post("/products/:id", authenticateToken, getOneProduct); // get one product
router.post("/products", authenticateToken, getAllProducts); // get all products
router.post("/products-count", getAllProductsCount); // get all products and the total count in hydrogen
router.post("/update-engagement-product", updateEngagementProduct); // create or update engagement ring product
router.post("/registration-email", sendRegistrationEmail); // send registration email from hydrogen

export default router;
