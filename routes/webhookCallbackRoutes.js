import express from "express";
import {
  createMetaobjectCallback,
  createCollectionCallback,
  updateCollectionCallback,
  deleteCollectionCallback,
  metafieldDefinitionUpdateCallback,
  subscribeWebhook,
  unsubscribeWebhook,
  listAllSubscribedWebhooks,
  createCustomerCallback,
} from "../controllers/webhookCallbackController.js";

//router object
const router = express.Router();

// callback routes
router.post("/metaobject-create-callback", createMetaobjectCallback);
router.post("/create-collection-callback", createCollectionCallback);
router.post("/update-collection-callback", updateCollectionCallback);
router.post("/delete-collection-callback", deleteCollectionCallback);
router.post(
  "/metafield-definition-update-callback",
  metafieldDefinitionUpdateCallback
);
router.post("/create-customer-callback", createCustomerCallback);
router.post("/subscribe-webhook", subscribeWebhook);
router.post("/list-webhooks", listAllSubscribedWebhooks);
router.post("/unsubscribe-webhook", unsubscribeWebhook);

export default router;
